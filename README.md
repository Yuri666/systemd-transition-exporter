# systemd-transition-exporter

Prometheus exporter for monitoring selected systemd services and recording **service state transitions** with systemd timestamps.

The project is intended for availability/KPI calculations where current `ActiveState` alone is insufficient. The important data is the sequence and time of transitions.

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

In particular, `inactive`, `activating`, `deactivating`, `failed`, `reloading`, `maintenance`, and `unknown` are DOWN.

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
- Crash/restart tests for WAL and Remote Write checkpoint handling.
- systemd deployment unit.

## Critical design rules

### D-Bus loss is not service downtime

A D-Bus outage means that the collector temporarily cannot observe systemd. It does **not** prove that a monitored service stopped.

```text
systemd service state        -> service availability
system D-Bus connectivity    -> collector observability
```

The exporter keeps the last known service state while D-Bus is unavailable. When the connection returns, the journal is used to recover the transition history observed during the gap.

### D-Bus timeout is not D-Bus disconnect

The collector does not use an application-level `Peer.Ping` timeout as the transport disconnect detector. The godbus connection context is used instead, avoiding false disconnects when systemd is temporarily busy during service operations.

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

The collector event WAL is the durable boundary for events already accepted by the collector. The journal is the historical source used to fill a D-Bus observation gap.

## Repository layout

```text
cmd/systemd-transition-exporter/    CLI and application wiring
internal/config/                    YAML configuration
internal/model/                     snapshots, states and events
internal/systemd/                   D-Bus/systemd access and reconnect
internal/engine/                    transition detection and recovery
internal/wal/                       durable JSONL event log
internal/recovery/                  systemd journal recovery reader
internal/metrics/                   Prometheus exposition
internal/remote_write/              Prometheus Remote Write sender
configs/config.yaml                 example configuration
deploy/systemd-transition-exporter.service
```

## Building

```bash
go test ./...
go build ./...
mkdir -p bin
go build -o bin/systemd-transition-exporter ./cmd/systemd-transition-exporter
```

The Makefile provides `make test`, `make build`, `make check`, and `make clean`.

## Configuration

Example:

```yaml
server:
  listen: "0.0.0.0:9877"
  debug: false

services:
  - pcscf.service
  - scscf.service
  - icscf.service

systemd:
  reconnect_interval: 1s
  reconciliation_interval: 30s
  startup_recovery_interval: 24h

wal:
  enabled: true
  directory: /var/lib/systemd-transition-exporter/wal
  fsync: true

remote_write:
  enabled: false
  urls:
    - "http://prometheus-a:9090/api/v1/write"
    - "http://prometheus-b:9090/api/v1/write"
  batch_size: 100
  flush_interval: 1s
  retry_interval: 1s
  timeout: 10s
  checkpoint: /var/lib/systemd-transition-exporter/remote_write.checkpoint
  state_interval: 1m
  recovery_fill_interval: 1m
  recovery_window: 15m
  labels:
    environment: production
    site: lab01
    role: ims
```

`server.listen` is the HTTP listen address. `server.debug` enables the destructive D-Bus disconnect test endpoint and defaults to false. `services` contains the systemd units to monitor. `reconnect_interval` controls the delay between reconnect attempts after a real D-Bus disconnect, while `reconciliation_interval` controls periodic snapshots used to catch missed signals.

`wal.enabled`, `wal.directory` and `wal.fsync` control the durable collector event log. The WAL file is `<wal.directory>/events.jsonl`.

## Remote Write

Set `remote_write.enabled: true` and list one or more receivers to send service
state samples through Remote Write:

```yaml
remote_write:
  enabled: true
  urls:
    - "http://prometheus-a:9090/api/v1/write"
    - "http://prometheus-b:9090/api/v1/write"
```

Every URL gets its own worker, in-memory queue, retry state, heartbeat schedule
and durable checkpoint. Delivery is a broadcast: every transition is sent to
every receiver, while a slow or unavailable receiver cannot block healthy
ones. All other `remote_write` settings and static labels are shared.

The legacy single-receiver form remains supported without changing its
checkpoint:

```yaml
remote_write:
  enabled: true
  url: "http://prometheus-a:9090/api/v1/write"
```

Do not configure `url` and `urls` together. When converting an existing
single-receiver installation to `urls`, put the existing receiver first. Its
legacy checkpoint is copied once to the new URL-specific checkpoint; later URL
reordering is safe because checkpoint names use a stable URL hash.

Each destination Prometheus must have the Remote Write receiver enabled:

```text
--web.enable-remote-write-receiver
```

### Metrics sent through Remote Write

**The following metric is the historical service-availability metric delivered through Remote Write:**

```text
systemd_service_state{service="cups.service",...} 0 <timestamp_ms>
systemd_service_state{service="cups.service",...} 1 <timestamp_ms>
```

Its semantics are:

- `1` = `ActiveState=active` exactly;
- `0` = any other `ActiveState`;
- transition samples use the **actual systemd transition timestamp**;
- the timestamp is not replaced by the Remote Write request time;
- every detected transition is delivered, including multiple transitions between Prometheus scrapes;
- samples can contain arbitrary configured static labels in addition to `service`.

The same `systemd_service_state` metric is also used for the current-state heartbeat. Heartbeat samples use the current exporter time and do not advance the transition checkpoint.

Each sender retries failed delivery and maintains its own durable checkpoint of
the last successfully delivered transition sequence. A successful HTTP `2xx`
response advances only that receiver's checkpoint; failed requests do not.

### Required Prometheus receiver setup

Two things have to be enabled on the Prometheus side. The receiving endpoint is
a command line flag:

```text
prometheus --web.enable-remote-write-receiver
```

which makes `POST /api/v1/write` available, so each entry in
`remote_write.urls` is normally
`http://<prometheus>:9090/api/v1/write`.

Out-of-order ingestion, in contrast, is **not** a command line flag; it is a
field of `prometheus.yml`:

```yaml
storage:
  tsdb:
    out_of_order_time_window: 30m
```

Without it every sample older than the head of a series is rejected, which
discards exactly the history this exporter exists to deliver: recovered
transitions and republished slots. See
[Multiple transitions between observations](#multiple-transitions-between-observations)
for how the window relates to `remote_write.recovery_window`.

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

The `service` label and metric name are controlled by the exporter and cannot be overridden through this map.

### Current-state heartbeat

`remote_write.state_interval` controls how often the exporter sends the current state of every monitored service. The default is one minute.

A heartbeat:

- is sent immediately after the initial systemd snapshot;
- is sent immediately after a successful D-Bus reconnect/snapshot;
- is then sent periodically according to `state_interval`;
- does not increment transition counters;
- does not receive a transition sequence number;
- does not change the transition WAL/checkpoint;
- uses the current timestamp when sent.

This prevents a long-running service time series from disappearing because no new sample was received for an extended period.

A heartbeat is suppressed while undelivered transitions are still queued. A
sample stamped with the current time would raise the maximum timestamp of the
series, and the pending transitions behind it — which are older by definition —
would then be refused as out of order. History is therefore always delivered
first, and the present state only afterwards.

## Remote Write outage

A destination outage is handled independently like a collector outage for that
receiver, because it missed both transitions and heartbeats:

- transitions accumulate in the durable WAL and in the send queue, and no
  current-time sample is published while they are pending;
- a single delivery attempt is bounded, so the sender never blocks the queue for
  the whole duration of the outage; the batch is retained and retried;
- when delivery succeeds again, the exporter republishes the current recovery
  slot from the journal exactly as it does on startup, which restores the
  continuity of every monitored service — including services that stayed down
  and would otherwise leave a gap in the graph;
- a request rejected with HTTP `4xx` is dropped from the send queue instead of
  being retried forever, because an unchanged payload would be rejected again
  and would block every later transition. The events remain in the WAL and
  `systemd_transition_exporter_remote_write_dropped_events_total` is increased.
  A growing counter almost always means the receiver accepts no out-of-order
  samples, or accepts a window smaller than one recovery slot.

Other configured receivers continue receiving transitions and heartbeats
during the outage. Recovery, ordering and checkpoint advancement are maintained
separately for each target.

## Prometheus metrics (`/metrics`)

The `/metrics` endpoint intentionally exposes **exporter/transport diagnostics only**. Service state, transition counters and transition timestamps are not exposed through `/metrics`; they are delivered through Remote Write to avoid creating duplicate Prometheus series from scrape and Remote Write.

### D-Bus metrics

```text
# HELP systemd_transition_exporter_dbus_connected Whether the exporter is currently connected to the system D-Bus (1 connected, 0 disconnected).
# TYPE systemd_transition_exporter_dbus_connected gauge
systemd_transition_exporter_dbus_connected 1
```

**`systemd_transition_exporter_dbus_connected`** — current exporter connectivity to the system D-Bus. `1` means connected, `0` means disconnected.

```text
# HELP systemd_transition_exporter_dbus_disconnects_total Total number of system D-Bus disconnections observed by the exporter.
# TYPE systemd_transition_exporter_dbus_disconnects_total counter
systemd_transition_exporter_dbus_disconnects_total 0
```

**`systemd_transition_exporter_dbus_disconnects_total`** — total number of real D-Bus disconnections observed since exporter start.

```text
# HELP systemd_transition_exporter_dbus_last_change_timestamp_seconds Unix timestamp of the last system D-Bus connection state change.
# TYPE systemd_transition_exporter_dbus_last_change_timestamp_seconds gauge
systemd_transition_exporter_dbus_last_change_timestamp_seconds 1787550000
```

**`systemd_transition_exporter_dbus_last_change_timestamp_seconds`** — Unix timestamp, in seconds, of the most recent D-Bus connected/disconnected state change.

```text
# HELP systemd_transition_exporter_dbus_disconnected_seconds Duration in seconds of the current system D-Bus disconnection, or 0 when connected.
# TYPE systemd_transition_exporter_dbus_disconnected_seconds gauge
systemd_transition_exporter_dbus_disconnected_seconds 0
```

**`systemd_transition_exporter_dbus_disconnected_seconds`** — duration of the current D-Bus outage in seconds. `0` while connected.

### Remote Write delivery metrics

These metrics are **only exporter diagnostics exposed by `/metrics`**. They are not the service-state time series sent through Remote Write.

```text
# HELP systemd_transition_exporter_remote_write_successful_requests_total Total number of successful Remote Write HTTP requests.
# TYPE systemd_transition_exporter_remote_write_successful_requests_total counter
systemd_transition_exporter_remote_write_successful_requests_total{target="8bf3a21c54d0"} 10
```

**`systemd_transition_exporter_remote_write_successful_requests_total`** — number of Remote Write HTTP requests completed with a `2xx` response for each target.

```text
# HELP systemd_transition_exporter_remote_write_failed_requests_total Total number of failed Remote Write HTTP requests or rejected requests.
# TYPE systemd_transition_exporter_remote_write_failed_requests_total counter
systemd_transition_exporter_remote_write_failed_requests_total{target="8bf3a21c54d0"} 2
```

**`systemd_transition_exporter_remote_write_failed_requests_total`** — number of failed, rejected, or otherwise unsuccessful Remote Write attempts. This can include attempts that are subsequently retried.

```text
# HELP systemd_transition_exporter_remote_write_retries_total Total number of Remote Write retry attempts.
# TYPE systemd_transition_exporter_remote_write_retries_total counter
systemd_transition_exporter_remote_write_retries_total{target="8bf3a21c54d0"} 2
```

**`systemd_transition_exporter_remote_write_retries_total`** — number of retry attempts made after a failed Remote Write request.

```text
# HELP systemd_transition_exporter_remote_write_samples_sent_total Total number of samples accepted by the Remote Write endpoint in successful requests.
# TYPE systemd_transition_exporter_remote_write_samples_sent_total counter
systemd_transition_exporter_remote_write_samples_sent_total{target="8bf3a21c54d0"} 123
```

**`systemd_transition_exporter_remote_write_samples_sent_total`** — total number of samples contained in successfully accepted Remote Write requests. It counts samples, not unique transitions.

```text
# HELP systemd_transition_exporter_remote_write_dropped_events_total Total number of transition events dropped after a permanent Remote Write rejection.
# TYPE systemd_transition_exporter_remote_write_dropped_events_total counter
systemd_transition_exporter_remote_write_dropped_events_total{target="8bf3a21c54d0"} 0
```

**`systemd_transition_exporter_remote_write_dropped_events_total`** — transition events removed from a target's send queue after that receiver rejected them with HTTP `4xx`. They stay in the WAL. Any non-zero value means transition history did not reach that target, usually because the out-of-order window is too small.

The `target` label is the stable first 12 hexadecimal characters of the URL
SHA-256 hash. The exporter logs the target-to-URL mapping at startup without
putting full receiver URLs into every diagnostic metric.

### Important distinction

The complete metric set is therefore:

| Metric | `/metrics` scrape | Remote Write | Purpose |
|---|---:|---:|---|
| `systemd_service_state` | **No** | **Yes** | Service availability and historical transitions |
| `systemd_service_transitions_total` | **No** | No | Internal transition counter used by the exporter implementation/history |
| `systemd_service_last_transition_timestamp_seconds` | **No** | No | Internal last-transition timestamp |
| `systemd_transition_exporter_dbus_connected` | **Yes** | No | D-Bus connectivity |
| `systemd_transition_exporter_dbus_disconnects_total` | **Yes** | No | D-Bus disconnect counter |
| `systemd_transition_exporter_dbus_last_change_timestamp_seconds` | **Yes** | No | Last D-Bus state-change timestamp |
| `systemd_transition_exporter_dbus_disconnected_seconds` | **Yes** | No | Current D-Bus outage duration |
| `systemd_transition_exporter_remote_write_successful_requests_total` | **Yes** | No | Successful Remote Write requests |
| `systemd_transition_exporter_remote_write_failed_requests_total` | **Yes** | No | Failed Remote Write requests |
| `systemd_transition_exporter_remote_write_retries_total` | **Yes** | No | Remote Write retries |
| `systemd_transition_exporter_remote_write_samples_sent_total` | **Yes** | No | Samples accepted by Remote Write |
| `systemd_transition_exporter_remote_write_dropped_events_total` | **Yes** | No | Transitions dropped after a permanent rejection |
| `systemd_transition_exporter_recovery_attempts_total` | **Yes** | No | Aligned recovery slots attempted |
| `systemd_transition_exporter_recovery_slot_start_timestamp_seconds` | **Yes** | No | Start of the last recovered slot |
| `systemd_transition_exporter_recovery_slot_end_timestamp_seconds` | **Yes** | No | End of the last journal query |
| `systemd_transition_exporter_recovery_uncovered_seconds` | **Yes** | No | Requested gap intentionally excluded before the current slot |

> **Note:** `systemd_service_transitions_total` and `systemd_service_last_transition_timestamp_seconds` are part of the historical service metric model/WAL state but are deliberately not emitted by the current `/metrics` handler. The authoritative service-state timeline delivered to Prometheus is `systemd_service_state` through Remote Write.

## Running manually

```bash
go build -o bin/systemd-transition-exporter ./cmd/systemd-transition-exporter
./bin/systemd-transition-exporter --config ./configs/config.yaml
```

Metrics endpoint:

```text
http://127.0.0.1:9877/metrics
```

Health endpoints:

```text
/health
/ready
```

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

There is deliberately no `Peer.Ping` timeout in this state machine. A slow systemd operation must not be interpreted as a D-Bus outage.

## Transition timestamps

The exporter uses:

- `ActiveEnterTimestamp`;
- `ActiveExitTimestamp`;
- `ActiveEnterTimestampMonotonic`;
- `ActiveExitTimestampMonotonic`.

Wall-clock transition timestamps are stored in microseconds internally and exported in milliseconds for Remote Write samples and seconds where Prometheus metric names require seconds.

## Multiple transitions between observations

The engine compares previous and current systemd enter/exit timestamps and emits newly observed transitions in timestamp order, allowing multiple transitions to be detected between observations.

Systemd unit properties are not a complete historical event log. Therefore transitions during a D-Bus monitoring gap are recovered from journald.

Recovery is deliberately limited to the current local-time slot. With the
default `remote_write.recovery_window: 15m`, an exporter becoming ready at
`15:14` reads transition events only from `[15:00, 15:14)`. An outage beginning
at `14:47` does not cause the closed `14:45..15:00` slot to be rewritten; the
excluded 13 minutes are exposed as
`systemd_transition_exporter_recovery_uncovered_seconds`.

The state at the slot start is not guessed. systemd reports a start only for a
unit that was not active, so the state preceding the first recovered transition
is that transition inverted. When the slot contains no transition at all, the
state cannot have changed during it and the currently observed state applies to
the whole slot — on a first start with an empty WAL that state is read from
systemd before monitoring begins. Only when the current state is unavailable
does the exporter fall back to the latest lifecycle event before the slot, and a
unit with no lifecycle record at all is reported as down.

The recovered slot is then **republished as samples**, not left to
interpolation. Prometheus removes a series from instant queries once no sample
is newer than its lookback delta (5 minutes by default), so an exporter outage
longer than that would otherwise leave a hole even though the state is known.
`remote_write.recovery_fill_interval` controls the density and must stay below
the lookback delta; the default is `1m`.

Each recovered slot therefore contains:

- one sample per `recovery_fill_interval` tick carrying the effective state;
- one sample at the exact timestamp of every recovered transition.

Samples inside the slot may be older than data Prometheus already holds, for
example when only the last minutes of the slot were missing. Prometheus rejects
those unless `storage.tsdb.out_of_order_time_window` is configured, as described
in [Required Prometheus receiver setup](#required-prometheus-receiver-setup).
The window has to cover both the slot itself and the delay with which the
exporter may republish it, so keep it comfortably above
`remote_write.recovery_window`.

Delivery of out-of-order samples can be verified from both sides:
`prometheus_tsdb_out_of_order_samples_appended_total` grows on Prometheus, while
`systemd_transition_exporter_remote_write_dropped_events_total` stays at zero on
the exporter.

## Host reboot

The exporter reads:

```text
/proc/sys/kernel/random/boot_id
```

A boot ID change identifies a host reboot. The latest state and boot ID are
persisted independently from transition events, so a service that was already
UP before the exporter started can still be recognized after reboot. Services
that were UP receive a synthetic DOWN event at the current host boot time.

The kernel spells the boot ID with hyphens while the journal's `_BOOT_ID` field
does not, so both are normalized before comparison. Without this, every restart
following a journal recovery was misread as a reboot.

If the boot predates the current recovery slot, the downtime is not
republished: the slot rule forbids writing into a closed interval. The boot ID
is adopted and the current state is re-established from the next snapshot.

## WAL

The collector WAL is append-only JSON Lines:

```text
/var/lib/systemd-transition-exporter/wal/events.jsonl
```

Each record contains the event sequence, service, state, wall-clock timestamp, monotonic timestamp, boot ID, source and systemd state information.

The latest observed service state is stored separately in:

```text
/var/lib/systemd-transition-exporter/wal/state.json
```

The WAL provides durable storage of events already detected by the collector
and replay on startup. Each Remote Write target has a separate delivery
checkpoint named from the configured checkpoint base plus its stable target ID,
for example `remote_write.checkpoint.8bf3a21c54d0`. Heartbeat samples do not
advance transition checkpoints.

Remote Write is deliberately **at-least-once per target**. A target checkpoint
advances only after that receiver returns `2xx` and the checkpoint file has
been durably persisted. A crash in the narrow interval after receiver
acceptance but before checkpoint persistence can cause a sample to be sent
again to that receiver after restart.

## Debug endpoint

For controlled D-Bus recovery testing, when debug functionality is enabled:

```yaml
server:
  debug: true
```

force a disconnect of the exporter's current D-Bus connection with:

```bash
curl -X POST http://127.0.0.1:9877/debug/dbus/disconnect
```

The endpoint disconnects only the exporter's D-Bus connection. It does not stop system D-Bus or systemd. Do not enable it on an externally accessible production listener.

## systemd installation

The deployment unit is:

```text
deploy/systemd-transition-exporter.service
```

It expects the binary at `/usr/local/bin/systemd-transition-exporter` and configuration at `/etc/systemd-transition-exporter/config.yaml`.

Typical installation:

```bash
sudo install -d /etc/systemd-transition-exporter
sudo install -d /var/lib/systemd-transition-exporter/wal
sudo install -m 0755 bin/systemd-transition-exporter /usr/local/bin/systemd-transition-exporter
sudo install -m 0644 configs/config.yaml /etc/systemd-transition-exporter/config.yaml
sudo install -m 0644 deploy/systemd-transition-exporter.service /etc/systemd/system/systemd-transition-exporter.service
sudo systemctl daemon-reload
sudo systemctl enable --now systemd-transition-exporter
```

## Prometheus scrape configuration

The `/metrics` endpoint is intended for exporter health and transport/delivery diagnostics:

```yaml
scrape_configs:
  - job_name: systemd-transition-exporter
    scrape_interval: 1m
    static_configs:
      - targets:
          - "127.0.0.1:9877"
```

Historical service transitions are delivered independently through Remote Write. The Prometheus scrape interval therefore does not determine transition detection precision.

## Testing

Run all unit tests:

```bash
go test ./...
```

The test suite includes Remote Write delivery, retry/failure handling, durable checkpoint behavior and crash/restart scenarios.

For a clean validation:

```bash
make check
```

## Roadmap

### Foundation

- project structure;
- configuration;
- D-Bus unit discovery;
- transition engine;
- Prometheus endpoint;
- durable event WAL;
- build/deployment skeleton.

### Resilient observation

- D-Bus reconnect;
- explicit D-Bus connectivity metrics;
- periodic reconciliation and reconciliation after reconnect;
- host reboot detection.

### Gap recovery

- journald reader;
- identify systemd unit start/stop events;
- recover every transition during a D-Bus outage;
- deduplicate D-Bus and journal events;
- preserve exact event timestamps and ordering.

### Durable delivery

- WAL replay;
- durable Remote Write checkpoints;
- Remote Write retries;
- recovery after collector crash;
- current-state heartbeat and startup/reconnect state publication.

### Production hardening

- WAL rotation/retention;
- integration tests against a real systemd instance;
- load tests with large unit sets;
- packaging and installation automation.
