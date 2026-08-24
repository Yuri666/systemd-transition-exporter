package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
	"github.com/Yuri666/systemd-transition-exporter/internal/remote_write"
)

type Registry struct {
	mu     sync.RWMutex
	states map[string]model.AvailabilityState
	up     map[string]uint64
	down   map[string]uint64
	last   map[string]int64

	dbusConnected      bool
	dbusDisconnects    uint64
	dbusLastChangeMS   int64
	dbusDisconnectedAt time.Time

	remoteWrite remote_write.Stats
}

func New() *Registry {
	return &Registry{
		states: make(map[string]model.AvailabilityState),
		up:     make(map[string]uint64),
		down:   make(map[string]uint64),
	}
}

func (r *Registry) SetState(service string, state model.AvailabilityState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = make(map[string]model.AvailabilityState)
	}
	r.states[service] = state
}

func (r *Registry) Event(e model.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = make(map[string]model.AvailabilityState)
	}
	if r.up == nil {
		r.up = make(map[string]uint64)
	}
	if r.down == nil {
		r.down = make(map[string]uint64)
	}
	if r.last == nil {
		r.last = make(map[string]int64)
	}
	r.states[e.Service] = e.State
	r.last[e.Service] = e.EventTimeUnixMS
	if e.State == model.StateUp {
		r.up[e.Service]++
	}
	if e.State == model.StateDown {
		r.down[e.Service]++
	}
}

// SetDBusConnected records collector connectivity to the system D-Bus.
// D-Bus loss is deliberately independent from service availability.
func (r *Registry) SetDBusConnected(connected bool, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dbusConnected == connected {
		return
	}
	r.dbusConnected = connected
	r.dbusLastChangeMS = at.UnixMilli()
	if connected {
		r.dbusDisconnectedAt = time.Time{}
	} else {
		r.dbusDisconnects++
		r.dbusDisconnectedAt = at
	}
}

// SetRemoteWriteStats publishes a snapshot of the Remote Write delivery
// counters. It is intentionally a snapshot: the sender remains independent
// from the Prometheus exposition path.
func (r *Registry) SetRemoteWriteStats(stats remote_write.Stats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.remoteWrite = stats
}

func (r *Registry) Handler(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	// Prometheus exposition metadata. Each metric family has exactly one HELP
	// and TYPE declaration before its samples.
	fmt.Fprintln(w, "# HELP systemd_service_state Current availability state of the systemd service (1 for active, 0 otherwise).")
	fmt.Fprintln(w, "# TYPE systemd_service_state gauge")
	fmt.Fprintln(w, "# HELP systemd_service_transitions_total Total number of observed systemd service state transitions, partitioned by resulting state.")
	fmt.Fprintln(w, "# TYPE systemd_service_transitions_total counter")
	fmt.Fprintln(w, "# HELP systemd_service_last_transition_timestamp_seconds Unix timestamp of the last observed systemd service state transition.")
	fmt.Fprintln(w, "# TYPE systemd_service_last_transition_timestamp_seconds gauge")

	for service, state := range r.states {
		v := 0
		if state == model.StateUp {
			v = 1
		}
		fmt.Fprintf(w, "systemd_service_state{service=%q} %d\n", service, v)
		fmt.Fprintf(w, "systemd_service_transitions_total{service=%q,state=\"up\"} %d\n", service, r.up[service])
		fmt.Fprintf(w, "systemd_service_transitions_total{service=%q,state=\"down\"} %d\n", service, r.down[service])
		fmt.Fprintf(w, "systemd_service_last_transition_timestamp_seconds{service=%q} %g\n", service, float64(r.last[service])/1000)
	}

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_dbus_connected Whether the exporter is currently connected to the system D-Bus (1 connected, 0 disconnected).")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_dbus_connected gauge")
	fmt.Fprintln(w, "# HELP systemd_transition_exporter_dbus_disconnects_total Total number of system D-Bus disconnections observed by the exporter.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_dbus_disconnects_total counter")
	fmt.Fprintln(w, "# HELP systemd_transition_exporter_dbus_last_change_timestamp_seconds Unix timestamp of the last system D-Bus connection state change.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_dbus_last_change_timestamp_seconds gauge")
	fmt.Fprintln(w, "# HELP systemd_transition_exporter_dbus_disconnected_seconds Duration in seconds of the current system D-Bus disconnection, or 0 when connected.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_dbus_disconnected_seconds gauge")

	dbusState := 0
	if r.dbusConnected {
		dbusState = 1
	}
	fmt.Fprintf(w, "systemd_transition_exporter_dbus_connected %d\n", dbusState)
	fmt.Fprintf(w, "systemd_transition_exporter_dbus_disconnects_total %d\n", r.dbusDisconnects)
	fmt.Fprintf(w, "systemd_transition_exporter_dbus_last_change_timestamp_seconds %g\n", float64(r.dbusLastChangeMS)/1000)

	disconnectedSeconds := 0.0
	if !r.dbusConnected && !r.dbusDisconnectedAt.IsZero() {
		disconnectedSeconds = time.Since(r.dbusDisconnectedAt).Seconds()
	}
	fmt.Fprintf(w, "systemd_transition_exporter_dbus_disconnected_seconds %g\n", disconnectedSeconds)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_remote_write_successful_requests_total Total number of successful Remote Write HTTP requests.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_remote_write_successful_requests_total counter")
	fmt.Fprintln(w, "# HELP systemd_transition_exporter_remote_write_failed_requests_total Total number of failed Remote Write HTTP requests or rejected requests.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_remote_write_failed_requests_total counter")
	fmt.Fprintln(w, "# HELP systemd_transition_exporter_remote_write_retries_total Total number of Remote Write retry attempts.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_remote_write_retries_total counter")
	fmt.Fprintln(w, "# HELP systemd_transition_exporter_remote_write_samples_sent_total Total number of samples accepted by the Remote Write endpoint in successful requests.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_remote_write_samples_sent_total counter")

	fmt.Fprintf(w, "systemd_transition_exporter_remote_write_successful_requests_total %d\n", r.remoteWrite.SuccessfulRequests)
	fmt.Fprintf(w, "systemd_transition_exporter_remote_write_failed_requests_total %d\n", r.remoteWrite.FailedRequests)
	fmt.Fprintf(w, "systemd_transition_exporter_remote_write_retries_total %d\n", r.remoteWrite.Retries)
	fmt.Fprintf(w, "systemd_transition_exporter_remote_write_samples_sent_total %d\n", r.remoteWrite.SentSamples)
}
