# systemd-transition-exporter

Prometheus exporter for monitoring selected systemd services and recording **service state transitions** with systemd timestamps.

The project is intended for availability/KPI calculations where it is not sufficient to expose only the current `ActiveState`. The important data is the sequence and exact time of transitions such as:

```text
UP   -> DOWN @ 12:01:10
DOWN -> UP   @ 12:01:12
UP   -> DOWN @ 12:03:40
DOWN -> UP   @ 12:04:00
```

The exporter is designed for IMS infrastructure such as `pcscf.service`, `scscf.service`, and `icscf.service`, but it is not limited to those units.

## Current implementation status

This repository is being developed in phases.

Implemented in the current phase:

- YAML configuration.
- Go CLI under `cmd/systemd-transition-exporter`.
- systemd D-Bus access through `github.com/godbus/dbus/v5`.
- Monitoring of configured units through `PropertiesChanged`.
- Exact transition timestamps taken from systemd `ActiveEnterTimestamp` / `ActiveExitTimestamp` and monotonic counterparts.
- State engine with monotonically increasing event sequence numbers.
- Detection of a host reboot through `/proc/sys/kernel/random/boot_id`.
- Host reboot is treated as service downtime for services that were UP before reboot.
- D-Bus health checking with `org.freedesktop.DBus.Peer.Ping` on the **systemd manager peer** (`org.freedesktop.systemd1`, `/org/freedesktop/systemd1`).
- D-Bus reconnect loop.
- Separate D-Bus connectivity metrics. D-Bus loss does **not** automatically turn a monitored service DOWN.
- Durable JSONL WAL for detected transition events.
- Prometheus `/metrics`, `/health`, and `/ready` endpoints.
- systemd deployment unit.

Not yet production-complete:

- journald-based recovery of transitions that occurred while D-Bus was unavailable;
- durable WAL replay/checkpointing and bounded WAL rotation;
- final Prometheus metric model for exposing every transition/recovery event;
- full integration tests against a real systemd instance;
- packaging and installation automation.

## Important design rule: D-Bus loss is not service downtime

A D-Bus outage means that the collector temporarily cannot observe systemd. It does not prove that the monitored service stopped.

Therefore the exporter keeps the last known service state while D-Bus is unavailable and exposes D-Bus connectivity separately:

```text
systemd service state        -> service availability
system D-Bus connectivity    -> collector observability
```

After reconnect, reconciliation restores the current systemd state. Future work in `internal/journal` will recover the complete transition history from the gap.

## Architecture

```text
                    systemd
                       |
                 system D-Bus
                       |
          +------------+-------------+
          |                          |
    PropertiesChanged          systemd Peer.Ping()
          |                          |
          +------------+-------------+
                       |
                internal/systemd
                       |
                       v
                 state snapshot
                       |
                       v
                 internal/engine
                       |
              transition events
                  /         \
                 v           v
          internal/wal    internal/metrics
                 |           |
                 v           v
             durable      /metrics
              events
```

The planned recovery path is:

```text
D-Bus outage
     |
     v
systemd journal
     |
     v
internal/journal
     |
     v
recovered events
     |
     v
state engine + WAL + metrics
```

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
    engine.go                transition detection
    engine_test.go           state-engine tests

internal/wal/
    wal.go                   durable JSONL event log

internal/metrics/
    metrics.go               Prometheus exposition

configs/
    config.yaml              example configuration

deploy/
    systemd-transition-exporter.service
```

## Building

### Go package build check

This command checks and builds all Go packages:

```bash
go test ./...
go build ./...
```

**Important:** `go build ./...` is a package build check. With multiple packages, it is not the command to rely on for the location/name of the final executable.

### Build the actual executable

Use:

```bash
mkdir -p bin
go build -o bin/systemd-transition-exporter ./cmd/systemd-transition-exporter
```

The executable will then be:

```text
bin/systemd-transition-exporter
```

### Makefile

The repository also provides convenient targets:

```bash
make test
make build
make check
make clean
```

`make build` produces:

```text
bin/systemd-transition-exporter
```

`make check` runs tests and then builds that executable explicitly.

## Configuration

Example: `configs/config.yaml`:

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

### `server.listen`

HTTP listen address. The default is `127.0.0.1:9877` if omitted.

### `services`

List of systemd unit names to monitor. At least one unit is required.

Example:

```yaml
services:
  - pcscf.service
```

### `systemd.reconnect_interval`

Delay between reconnect attempts after an established D-Bus connection is lost. The current default is `1s`.

### `systemd.reconciliation_interval`

Reserved for the periodic reconciliation mechanism. The complete periodic reconciliation/recovery implementation will be completed together with journal recovery.

### `wal.enabled`

Enables durable event logging.

### `wal.directory`

Directory containing the event WAL. The current implementation uses:

```text
<directory>/events.jsonl
```

### `wal.fsync`

When enabled, each appended event is followed by `fsync`. This provides stronger durability at the cost of additional I/O.

## Running manually

For a locally built executable:

```bash
./bin/systemd-transition-exporter --config ./configs/config.yaml
```

For the default production configuration path:

```bash
./bin/systemd-transition-exporter \
  --config /etc/systemd-transition-exporter/config.yaml
```

The HTTP endpoint is then available at:

```text
http://127.0.0.1:9877/metrics
```

## Prometheus metrics

### Service state

Current state:

```text
systemd_service_state{service="pcscf.service"} 1
```

`1` means UP/active and `0` means DOWN/inactive according to the current availability mapping.

Transition counters:

```text
systemd_service_transitions_total{service="pcscf.service",state="up"} ...
systemd_service_transitions_total{service="pcscf.service",state="down"} ...
```

Last transition timestamp:

```text
systemd_service_last_transition_timestamp_seconds{service="pcscf.service"} ...
```

### D-Bus connectivity

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

The service state is intentionally not changed just because D-Bus is unavailable.

## D-Bus monitoring

The exporter does not infer D-Bus loss from the absence of systemd signals. It actively checks the **systemd D-Bus manager peer** with:

```text
destination: org.freedesktop.systemd1
object path: /org/freedesktop/systemd1
interface:  org.freedesktop.DBus.Peer
method:     Ping
```

This is deliberately not a `Peer.Ping` call to the `org.freedesktop.DBus` bus-daemon destination. Some hosts apply D-Bus policy that rejects that call even though the connection is healthy. The health check therefore targets the actual systemd manager object.

The current health-check interval is 1 second and the current ping timeout is 500 ms.

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
systemd Peer.Ping
   |
   v
CONNECTED
   |
   +---- PropertiesChanged ----> snapshot -> engine
   |
   +---- Ping/transport error -> DISCONNECTED
                                  |
                                  v
                               reconnect
```

Initial connection/setup errors are logged as errors and are not counted as a loss of an already-established connection.

## Transition timestamps

The exporter uses systemd properties:

- `ActiveEnterTimestamp`;
- `ActiveExitTimestamp`;
- `ActiveEnterTimestampMonotonic`;
- `ActiveExitTimestampMonotonic`.

Wall-clock transition timestamps are stored in microseconds internally and exported in seconds where Prometheus requires seconds.

Monotonic timestamps are retained in the event model for ordering and diagnostic purposes.

## Multiple transitions between observations

The engine can compare the previous and current systemd enter/exit timestamps and emit newly observed transitions in timestamp order.

However, systemd unit properties only retain the timestamps represented by the current unit state. They are not a complete historical event log. Therefore the final design cannot rely on D-Bus snapshots alone to recover an arbitrary number of transitions during a monitoring gap.

That is the reason journald recovery is a required next phase.

## Host reboot

The exporter reads:

```text
/proc/sys/kernel/random/boot_id
```

A boot ID change identifies a host reboot. Reboot is explicitly treated as downtime for services that were UP before the reboot.

Conceptually:

```text
pcscf UP
scscf UP

       HOST REBOOT
            |
            v
pcscf DOWN
scscf DOWN
```

When systemd starts the services again, normal systemd transition events establish the subsequent UP timestamps.

## WAL

The current WAL is append-only JSON Lines:

```text
/var/lib/systemd-transition-exporter/wal/events.jsonl
```

Each record contains the event sequence, service, state, wall-clock timestamp, monotonic timestamp, boot ID, source and systemd state information.

The WAL currently provides durable storage of events already detected by the collector. It is not yet the complete remote-write queue/replay mechanism from the final design.

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

The exporter is intentionally designed so that Prometheus does not need to poll systemd every second. The collector observes transitions continuously and persists the detected events independently of the Prometheus scrape interval.

## Development workflow

Before committing changes:

```bash
make check
```

which is equivalent to running the package tests and building the actual executable.

For a clean rebuild:

```bash
make clean
make check
```

## Roadmap

### Phase 1/2 — completed foundation

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
