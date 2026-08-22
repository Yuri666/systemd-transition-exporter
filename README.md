# systemd-transition-exporter

Prometheus exporter for monitoring selected systemd services and recording **service state transitions** with systemd timestamps.

The project is intended for availability/KPI calculations where current `ActiveState` alone is insufficient. The important data is the sequence and time of transitions such as:

```text
UP   -> DOWN @ 12:01:10
DOWN -> UP   @ 12:01:12
UP   -> DOWN @ 12:03:40
DOWN -> UP   @ 12:04:00
```

The exporter is intended for IMS infrastructure such as `pcscf.service`, `scscf.service`, and `icscf.service`, but is not limited to those units.

## Core design goal

The primary output is the **historical service state timeline**, delivered to Prometheus through Remote Write with the original event timestamp. Prometheus scrape frequency therefore does not limit transition resolution.

For example, if a service changes state twice between two Prometheus scrapes:

```text
19:15:26  DOWN
19:15:36  UP
```

the exporter sends two Remote Write samples with timestamps `19:15:26` and `19:15:36`, rather than reporting only the state observed at the next scrape.

The exporter also periodically sends the current state so that a continuously-running service remains present in the Prometheus time series. The current state is sent immediately after the initial systemd snapshot and after reconnect, then periodically according to `remote_write.state_interval`.

## Availability state mapping

The availability mapping is deliberately strict:

```text
systemd ActiveState="active"  -> UP
any other ActiveState           -> DOWN
```

In particular, the following are **DOWN**, not UP:

```text
inactive
activating
deactivating
failed
reloading
maintenance
unknown
```

This rule is used consistently by the transition engine and the Prometheus current-state exporter. The collector does not treat a transitional state such as `activating` as service availability.

## Current implementation status

Implemented:

- YAML configuration and Go CLI.
- systemd D-Bus access through `github.com/godbus/dbus/v5`.
- Monitoring of configured units through `PropertiesChanged`.
- Transition timestamps from systemd `ActiveEnterTimestamp` / `ActiveExitTimestamp` and monotonic counterparts.
- Strict availability mapping: only `ActiveState=active` is UP.
- State engine with monotonically increasing event sequence numbers.
- D-Bus reconnect after a real connection loss.
- Separate D-Bus connectivity metrics.
- Durable JSONL WAL for detected transition events.
- WAL replay on collector startup, restoring event counters, last state and sequence number.
- Journald recovery reader for transitions occurring during a detected D-Bus outage.
- Deduplication of recovered records against the already processed event stream.
- Prometheus `/metrics`, `/health`, and `/ready` endpoints.
- Prometheus Remote Write delivery with event timestamps.
- Remote Write checkpointing and retry after temporary destination failures.
- Configurable arbitrary static labels added to Remote Write series.
- Periodic current-state Remote Write heartbeat.
- Immediate current-state Remote Write after startup snapshot and D-Bus reconnect.
- systemd deployment unit.

Still requiring production hardening:

- reboot-gap reconciliation across old and new boots;
- WAL checkpoints, segmentation and bounded retention for the collector event WAL;
- full integration tests against a real systemd instance;
- packaging and installation automation.

## Critical design rules

### D-Bus loss is not service downtime

A D-Bus outage means that the collector temporarily cannot observe systemd. It does **not** prove that a monitored service stopped.

Therefore:

```text
systemd service state        -> service availability
system D-Bus connectivity    -> collector observability
```

The exporter keeps the last known service state while D-Bus is unavailable. When the connection returns, the journal is used to recover the transition history observed during the gap.

### D-Bus timeout is not D-Bus disconnect

A slow systemd operation must not cause a false D-Bus outage. The collector therefore does not use an application-level `Peer.Ping` timeout as the transport disconnect detector.

The godbus connection context is used instead. `godbus` documents `Conn.Context()` as the context cancelled when the connection is closed and `Conn.Connected()` as the connection-state check.

This distinction is important for service operations: starting/stopping a unit can temporarily make systemd slower without meaning that the D-Bus transport was lost.

## Architecture

```text
                         systemd
                            |
                     system D-Bus
                            |
                  PropertiesChanged
                            |
                            v
                    internal/systemd
                            |
                            v
                     state snapshots
                            |
                            v
                     internal/engine
                            |
                    transition events
                       /          \
                      v            v
               internal/wal    internal/metrics
                      |             |
                      v             v
                  durable       /metrics
                   events
                      |
                      v
              internal/remote_write
                 /           \
                /             \
       transition samples   state heartbeat
       event timestamp      current timestamp
                \             /
                 \           /
                  v         v
                    Prometheus

D-Bus outage:

       systemd journal
             |
             v
      internal/recovery
             |
             v
      missing transitions
             |
             v
       engine -> WAL -> remote_write
```

The collector event WAL is the durable boundary for events already accepted by the collector. The journal is the historical source used to fill a D-Bus observation gap. Remote Write provides delivery of the historical timeline to Prometheus.

## Repository layout

```text
cmd/systemd-transition-exporter/
    main.go                  CLI and application wiring

internal/config/
    config.go                YAML configuration

internal/model/
    model.go                 snapshots, states and events

internal/systemd/
    systemd.go               D-Bus/systemd access
    resilient.go             connection lifecycle and reconnect

internal/engine/
    engine.go                transition detection, replay and recovery

internal/wal/
    wal.go                   durable JSONL event log

internal/recovery/
    journal.go               systemd journal recovery reader

internal/metrics/
    metrics.go               Prometheus exposition

internal/remote_write/
    remote_write.go          Prometheus Remote Write sender

configs/
    config.yaml              example configuration

deploy/
    systemd-transition-exporter.service
```

## Building

Package validation:

```bash
go test ./...
go build ./...
```

`go build ./...` validates/builds packages. To create the actual executable use:

```bash
mkdir -p bin
go build -o bin/systemd-transition-exporter ./cmd/systemd-transition-exporter
```

The repository Makefile provides:

```bash
make test
make build
make check
make clean
```

`make check` runs tests and builds `bin/systemd-transition-exporter` explicitly.

## Configuration

Example `configs/config.yaml`:

```yaml
server:
  listen: "0.0.0.0:9877"

debug:
  enabled: false

services:
  - pcscf.service
  - scscf.service
  - icscf.service

systemd:
  reconnect_interval: 1s
  reconciliation_interval: 30s

wal:
  enabled: true
  directory: /var/lib/systemd-transition-exporter/wal
  fsync: true

remote_write:
  enabled: false
  url: "http://127.0.0.1:9090/api/v1/write"
  batch_size: 100
  flush_interval: 1s
  retry_interval: 1s
  timeout: 10s
  checkpoint: /var/lib/systemd-transition-exporter/remote_write.checkpoint
  state_interval: 1m
  labels:
    environment: production
    site: lab01
    role: ims
```

`server.listen` is the HTTP listen address. `services` contains the systemd units to monitor. `reconnect_interval` controls the delay between reconnect attempts after a real D-Bus disconnect.

`wal.enabled`, `wal.directory` and `wal.fsync` control the durable collector event log. The current WAL file is:

```text
<wal.directory>/events.jsonl
```

### Remote Write

Set `remote_write.enabled: true` to send service state samples to a Prometheus Remote Write receiver:

```yaml
remote_write:
  enabled: true
  url: "http://prometheus:9090/api/v1/write"
```

The destination Prometheus must have the Remote Write receiver enabled, for example with:

```text
--enable-feature=remote-write-receiver
```

The exporter sends transition samples using the **actual systemd transition timestamp**:

```text
systemd_service_state{service="cups.service",...} 0 <transition_timestamp_ms>
systemd_service_state{service="cups.service",...} 1 <transition_timestamp_ms>
```

These timestamps are not replaced by the time of the Remote Write request.

The sender retries failed delivery and maintains a checkpoint of the last successfully delivered transition sequence. The checkpoint does not advance for current-state heartbeat samples.

### Remote Write labels

`remote_write.labels` is a free-form YAML map. Every configured label is added to the Remote Write `systemd_service_state` series:

```yaml
remote_write:
  labels:
    environment: production
    site: ams01
    network: ims
    role: scscf
```

The `service` label and metric name are controlled by the exporter and must not be overridden through this map.

### Current-state heartbeat

`remote_write.state_interval` controls how often the exporter sends the current state of every monitored service. The default is one minute.

A heartbeat is **not a transition**. It:

- does not increment transition counters;
- does not receive a transition sequence number;
- does not change the transition WAL/checkpoint;
- uses the current timestamp when it is sent.

The first current-state sample is sent immediately after the initial systemd snapshot. The exporter does not wait for `state_interval` after startup. A new current-state sample is also sent immediately after a successful D-Bus reconnect/snapshot, followed by the normal heartbeat interval.

For example:

```text
19:00:00  exporter starts, service UP  -> state sample immediately
19:01:00                              -> heartbeat UP
19:02:00                              -> heartbeat UP
19:03:17  service STOP                 -> transition DOWN, timestamp 19:03:17
19:03:17+ state remains DOWN           -> transition sample already delivered
19:04:00                              -> heartbeat DOWN
19:05:00                              -> heartbeat DOWN
```

The heartbeat exists so that a long-running service remains represented in the Prometheus time series between transitions.

## Running manually

```bash
go build -o bin/systemd-transition-exporter ./cmd/systemd-transition-exporter
./bin/systemd-transition-exporter --config ./configs/config.yaml
```

The metrics endpoint is:

```text
http://127.0.0.1:9877/metrics
```

## Prometheus metrics

Current service state:

```text
systemd_service_state{service="pcscf.service"} 1
```

`1` means **exactly `ActiveState=active`** and `0` means every other systemd `ActiveState`.

Transition counters:

```text
systemd_service_transitions_total{service="pcscf.service",state="up"} ...
systemd_service_transitions_total{service="pcscf.service",state="down"} ...
```

Last transition timestamp:

```text
systemd_service_last_transition_timestamp_seconds{service="pcscf.service"} ...
```

D-Bus connectivity:

```text
systemd_transition_exporter_dbus_connected 1
systemd_transition_exporter_dbus_disconnects_total 0
systemd_transition_exporter_dbus_last_change_timestamp_seconds ...
systemd_transition_exporter_dbus_disconnected_seconds 0
```

During a real D-Bus outage:

```text
systemd_transition_exporter_dbus_connected 0
```

The service state is intentionally not changed merely because D-Bus is unavailable.

## D-Bus monitoring and reconnect

The connection lifecycle is:

```text
CONNECTING
   |
   v
ConnectSystemBus
   |
   v
Load configured units
   |
   v
Install PropertiesChanged match
   |
   v
Initial snapshots
   |
   v
CONNECTED
   |
   +---- PropertiesChanged ---> snapshot -> engine -> WAL -> remote_write
   |
   +---- connection context ---> DISCONNECTED
                                  |
                                  v
                               reconnect
                                  |
                                  v
                           journal recovery
```

There is deliberately **no `Peer.Ping` timeout in this state machine**. This prevents the false disconnect observed when a service start/stop temporarily made systemd busy.

## Transition timestamps

The exporter uses:

- `ActiveEnterTimestamp`;
- `ActiveExitTimestamp`;
- `ActiveEnterTimestampMonotonic`;
- `ActiveExitTimestampMonotonic`.

Wall-clock transition timestamps are stored in microseconds internally and exported in seconds where Prometheus requires seconds.

## Multiple transitions between observations

The engine compares the previous and current systemd enter/exit timestamps and emits newly observed transitions in timestamp order, allowing multiple transitions to be detected between observations.

Systemd unit properties are not a complete historical event log. Therefore arbitrary transitions during a D-Bus monitoring gap are recovered from journald.

## Host reboot

The exporter reads:

```text
/proc/sys/kernel/random/boot_id
```

A boot ID change identifies a host reboot. Reboot is explicitly treated as downtime for services that were UP before the reboot.

## WAL

The current collector WAL is append-only JSON Lines:

```text
/var/lib/systemd-transition-exporter/wal/events.jsonl
```

Each record contains the event sequence, service, state, wall-clock timestamp, monotonic timestamp, boot ID, source and systemd state information.

The WAL provides durable storage of events already detected by the collector and replay on startup. Remote Write has a separate delivery checkpoint. Heartbeat samples do not advance the transition checkpoint.

## Debug endpoint

For controlled D-Bus recovery testing, when debug functionality is enabled:

```yaml
debug:
  enabled: true
```

force a disconnect of the exporter's current D-Bus connection with:

```bash
curl -X POST http://127.0.0.1:9877/debug/dbus/disconnect
```

The endpoint disconnects only the exporter's D-Bus connection. It does not stop the system D-Bus daemon or systemd.

Do not enable the debug endpoint on an externally accessible production listener.

## systemd installation

The deployment unit is:

```text
deploy/systemd-transition-exporter.service
```

It expects the binary at:

```text
/usr/local/bin/systemd-transition-exporter
```

and configuration at:

```text
/etc/systemd-transition-exporter/config.yaml
```

A typical installation sequence is:

```bash
sudo install -d /etc/systemd-transition-exporter
sudo install -d /var/lib/systemd-transition-exporter/wal
sudo install -m 0755 bin/systemd-transition-exporter /usr/local/bin/systemd-transition-exporter
sudo install -m 0644 configs/config.yaml /etc/systemd-transition-exporter/config.yaml
sudo install -m 0644 deploy/systemd-transition-exporter.service /etc/systemd/system/systemd-transition-exporter.service
```

Create the service account before starting the unit:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin systemd-transition-exporter
sudo chown -R systemd-transition-exporter:systemd-transition-exporter /var/lib/systemd-transition-exporter
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now systemd-transition-exporter
sudo systemctl status systemd-transition-exporter
```

## Prometheus scrape configuration

The `/metrics` endpoint remains available for exporter health, current state and diagnostic counters:

```yaml
scrape_configs:
  - job_name: systemd-transition-exporter
    scrape_interval: 1m
    static_configs:
      - targets:
          - "127.0.0.1:9877"
```

Historical transition samples are delivered independently through Remote Write. Prometheus scrape interval therefore does not determine transition detection precision.

## Development workflow

Before committing changes:

```bash
make check
```

For a clean rebuild:

```bash
make clean
make check
```

## Roadmap

### Phase 1/2 — foundation

- project structure;
- configuration;
- D-Bus unit discovery;
- transition engine;
- Prometheus endpoint;
- durable event WAL;
- build/deployment skeleton.

### Phase 3 — resilient observation

- D-Bus reconnect;
- explicit D-Bus connectivity metrics;
- reconciliation after reconnect;
- host reboot detection.

### Phase 4 — gap recovery

- journald reader;
- identify systemd unit start/stop events;
- recover every transition during a D-Bus outage;
- deduplicate D-Bus and journal events;
- preserve exact event timestamps and ordering.

### Phase 5 — durable delivery

- WAL replay;
- checkpoints;
- WAL rotation/retention;
- Remote Write delivery;
- recovery after collector crash;
- current-state heartbeat and startup/reconnect state publication.

### Phase 6 — production hardening

- integration tests against systemd;
- load tests with large unit sets;
- bounded memory usage;
- security review of D-Bus permissions and systemd sandboxing;
- packaging.
