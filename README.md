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

## Current implementation status

Implemented:

- YAML configuration and Go CLI.
- systemd D-Bus access through `github.com/godbus/dbus/v5`.
- Monitoring of configured units through `PropertiesChanged`.
- Transition timestamps from systemd `ActiveEnterTimestamp` / `ActiveExitTimestamp` and monotonic counterparts.
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

The godbus connection context is used instead. `godbus` documents `Conn.Context()` as the context cancelled when the connection is closed and `Conn.Connected()` as the connection-state check. urlgodbus/dbus conn.gohttps://github.com/godbus/dbus/blob/master/conn.go

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

`1` means UP/active and `0` means DOWN/inactive according to the availability mapping.

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

Wall-clock timestamps are retained internally in microseconds and exported in seconds for Prometheus metrics. The event model also retains monotonic timestamps when available.

## Multiple transitions between observations

The engine compares the previous and current systemd enter/exit timestamps and can emit multiple newly observed transitions in chronological order.

However, systemd unit properties are not a historical event log. If D-Bus is unavailable, the current unit properties cannot reconstruct arbitrary `stop/start` cycles. That is why the exporter now queries journald after reconnect and imports `Started`/`Stopped`/`Failed` records from the outage interval.

Recovered records receive normal collector sequence numbers and are persisted to the same WAL. Records whose timestamp is not newer than the last processed event for the service are ignored, making overlap between realtime D-Bus events and journal recovery safe.

## WAL and restart recovery

When WAL is enabled, startup replays `events.jsonl` before D-Bus monitoring begins. This restores:

- the last known service availability state;
- transition counters;
- the last processed event timestamp;
- the durable sequence number.

This prevents sequence numbers from restarting at `1` after a collector restart and prevents Prometheus metrics from being reset merely because the exporter process was restarted.

The current WAL is intentionally simple append-only JSONL. Checkpointing, segmentation and bounded retention are production-hardening work still to be completed.

## Host reboot

The exporter reads:

```text
/proc/sys/kernel/random/boot_id
```

A boot ID change identifies a host reboot. Reboot is a required downtime condition: services that were UP before the reboot must be represented as DOWN during the reboot interval.

The final reboot reconciliation must preserve the previous boot's last service state and recover the first post-boot `Started` event. This is the next hardening step after the current journal-gap recovery implementation.

## systemd installation

The deployment unit is:

```text
deploy/systemd-transition-exporter.service
```

It expects:

```text
/usr/local/bin/systemd-transition-exporter
/etc/systemd-transition-exporter/config.yaml
```

Example installation:

```bash
sudo install -d /etc/systemd-transition-exporter
sudo install -d /var/lib/systemd-transition-exporter/wal
sudo install -m 0755 bin/systemd-transition-exporter /usr/local/bin/systemd-transition-exporter
sudo install -m 0644 configs/config.yaml /etc/systemd-transition-exporter/config.yaml
sudo install -m 0644 deploy/systemd-transition-exporter.service /etc/systemd/system/systemd-transition-exporter.service
```

Create the service account and grant ownership of the WAL directory:

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

The account must have sufficient access to the system D-Bus and journal. On systems with restrictive D-Bus/journald policy, those permissions must be configured explicitly.

## Prometheus scrape configuration

```yaml
scrape_configs:
  - job_name: systemd-transition-exporter
    scrape_interval: 1m
    static_configs:
      - targets:
          - "127.0.0.1:9877"
```

The collector observes transitions continuously, so Prometheus does not need to poll systemd every second. Prometheus can scrape once per minute while the collector preserves transition facts independently.

## Recovery test scenario

The intended resilience test is:

```text
1. collector connected to D-Bus
2. force real D-Bus outage
3. stop/start a monitored service several times
4. restore D-Bus
5. verify every stop/start appears in WAL and metrics
```

For example, during a five-minute gap:

```text
DOWN @ 12:00:30
UP   @ 12:00:40
DOWN @ 12:01:10
UP   @ 12:01:20
DOWN @ 12:02:00
UP   @ 12:02:30
```

All six events must be recovered after reconnect. A single final `ActiveState=active` is not sufficient.

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
- no false disconnect on slow systemd calls;
- host reboot detection.

### Phase 4 — gap recovery

- journald reader;
- recover every `Started`/`Stopped` transition during a D-Bus outage;
- deduplicate realtime and journal events;
- persist recovered events in the same WAL.

### Phase 5 — durable delivery

- WAL checkpoints;
- WAL rotation/retention;
- remote-write delivery/replay;
- crash recovery hardening.

### Phase 6 — production hardening

- reboot-gap reconciliation across boot IDs;
- integration tests against real systemd/journald;
- load tests with large unit sets;
- bounded memory usage;
- security review of D-Bus and journal permissions;
- packaging.
