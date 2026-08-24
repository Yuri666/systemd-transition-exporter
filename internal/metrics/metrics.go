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

	recoveryAttempts    uint64
	recoverySlotStartMS int64
	recoverySlotEndMS   int64
	uncoveredSeconds    float64
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

// RecordRecovery records the aligned slot selected for journal recovery.
// Any part of the requested observation gap before slotStart is intentionally
// left uncovered and reported separately.
func (r *Registry) RecordRecovery(observedFrom, slotStart, slotEnd time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recoveryAttempts++
	r.recoverySlotStartMS = slotStart.UnixMilli()
	r.recoverySlotEndMS = slotEnd.UnixMilli()
	r.uncoveredSeconds = 0
	if observedFrom.Before(slotStart) {
		r.uncoveredSeconds = slotStart.Sub(observedFrom).Seconds()
	}
}

func (r *Registry) Handler(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	// Service state, transition counters and transition timestamps are
	// delivered exclusively through Remote Write. They are intentionally not
	// exposed here because scraping the same series would create duplicate
	// time-series sources in Prometheus with conflicting timestamps.

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_dbus_connected Whether the exporter is currently connected to the system D-Bus (1 connected, 0 disconnected).")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_dbus_connected gauge")
	dbusState := 0
	if r.dbusConnected {
		dbusState = 1
	}
	fmt.Fprintf(w, "systemd_transition_exporter_dbus_connected %d\n", dbusState)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_dbus_disconnects_total Total number of system D-Bus disconnections observed by the exporter.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_dbus_disconnects_total counter")
	fmt.Fprintf(w, "systemd_transition_exporter_dbus_disconnects_total %d\n", r.dbusDisconnects)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_dbus_last_change_timestamp_seconds Unix timestamp of the last system D-Bus connection state change.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_dbus_last_change_timestamp_seconds gauge")
	fmt.Fprintf(w, "systemd_transition_exporter_dbus_last_change_timestamp_seconds %g\n", float64(r.dbusLastChangeMS)/1000)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_dbus_disconnected_seconds Duration in seconds of the current system D-Bus disconnection, or 0 when connected.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_dbus_disconnected_seconds gauge")
	disconnectedSeconds := 0.0
	if !r.dbusConnected && !r.dbusDisconnectedAt.IsZero() {
		disconnectedSeconds = time.Since(r.dbusDisconnectedAt).Seconds()
	}
	fmt.Fprintf(w, "systemd_transition_exporter_dbus_disconnected_seconds %g\n", disconnectedSeconds)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_remote_write_successful_requests_total Total number of successful Remote Write HTTP requests.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_remote_write_successful_requests_total counter")
	fmt.Fprintf(w, "systemd_transition_exporter_remote_write_successful_requests_total %d\n", r.remoteWrite.SuccessfulRequests)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_remote_write_failed_requests_total Total number of failed Remote Write HTTP requests or rejected requests.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_remote_write_failed_requests_total counter")
	fmt.Fprintf(w, "systemd_transition_exporter_remote_write_failed_requests_total %d\n", r.remoteWrite.FailedRequests)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_remote_write_retries_total Total number of Remote Write retry attempts.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_remote_write_retries_total counter")
	fmt.Fprintf(w, "systemd_transition_exporter_remote_write_retries_total %d\n", r.remoteWrite.Retries)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_remote_write_samples_sent_total Total number of samples accepted by the Remote Write endpoint in successful requests.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_remote_write_samples_sent_total counter")
	fmt.Fprintf(w, "systemd_transition_exporter_remote_write_samples_sent_total %d\n", r.remoteWrite.SentSamples)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_recovery_attempts_total Total number of aligned journal recovery slots attempted.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_recovery_attempts_total counter")
	fmt.Fprintf(w, "systemd_transition_exporter_recovery_attempts_total %d\n", r.recoveryAttempts)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_recovery_slot_start_timestamp_seconds Start of the last local-time recovery slot as a Unix timestamp.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_recovery_slot_start_timestamp_seconds gauge")
	fmt.Fprintf(w, "systemd_transition_exporter_recovery_slot_start_timestamp_seconds %g\n", float64(r.recoverySlotStartMS)/1000)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_recovery_slot_end_timestamp_seconds End of the last journal recovery query as a Unix timestamp.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_recovery_slot_end_timestamp_seconds gauge")
	fmt.Fprintf(w, "systemd_transition_exporter_recovery_slot_end_timestamp_seconds %g\n", float64(r.recoverySlotEndMS)/1000)

	fmt.Fprintln(w, "# HELP systemd_transition_exporter_recovery_uncovered_seconds Portion of the last requested observation gap before the current recovery slot that was intentionally not recovered.")
	fmt.Fprintln(w, "# TYPE systemd_transition_exporter_recovery_uncovered_seconds gauge")
	fmt.Fprintf(w, "systemd_transition_exporter_recovery_uncovered_seconds %g\n", r.uncoveredSeconds)
}
