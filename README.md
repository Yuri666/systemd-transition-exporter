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
- systemd deployment unit.

Still requiring production hardening:

- reboot-gap reconciliation across old and new boots;
- WAL checkpoints, segmentation and bounded retention;
- complete remote-write delivery/replay layer;
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
       engine -> WAL -> metrics
```

The WAL is the durable boundary for events already accepted by the collector. The journal is the historical source used to fill a D-Bus observation gap.

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
```

`server.listen` is the HTTP listen address. `services` contains the systemd units to monitor. `reconnect_interval` controls the delay between reconnect attempts after a real D-Bus disconnect.

`reconciliation_interval` is retained as a configuration field for the planned periodic reconciliation mechanism; journal recovery is currently triggered directly after a detected D-Bus gap.

`wal.enabled`, `wal.directory` and `wal.fsync` control the durable event log. The current WAL file is:

```text
<wal.directory>/events.jsonl
```

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
   +---- PropertiesChanged ---> snapshot -> engine -> WAL
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

Initial connection/setup errors are logged as errors and are not counted as a loss of an already-established connection.

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

The current WAL is append-only JSON Lines:

```text
/var/lib/systemd-transition-exporter/wal/events.jsonl
```

Each record contains the event sequence, service, state, wall-clock timestamp, monotonic timestamp, boot ID, source and systemd state information.

The WAL currently provides durable storage of events already detected by the collector and replay on startup. Segmentation, checkpoints and bounded retention remain future hardening work.

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

Example:

```yaml
scrape_configs:
  - job_name: systemd-transition-exporter
    scrape_interval: 1m
    static_configs:
      - targets:
          - "127.0.0.1:9877"
```

The exporter observes transitions continuously and persists detected events independently of the Prometheus scrape interval.

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
- remote-write delivery layer;
- recovery after collector crash.

### Phase 6 — production hardening

- integration tests against systemd;
- load tests with large unit sets;
- bounded memory usage;
- security review of D-Bus permissions and systemd sandboxing;
- packaging.
