package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type Registry struct {
	mu     sync.RWMutex
	states map[string]model.AvailabilityState
	up     map[string]uint64
	down   map[string]uint64
	last   map[string]int64

	dbusConnected       bool
	dbusDisconnects     uint64
	dbusLastChangeMS    int64
	dbusDisconnectedAt  time.Time
}

func New() *Registry {
	return &Registry{
		states: map[string]model.AvailabilityState{},
		up:     map[string]uint64{},
		down:   map[string]uint64{},
		dbusConnected: false,
	}
}

func (r *Registry) SetState(service string, state model.AvailabilityState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states[service] = state
}

func (r *Registry) Event(e model.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
// D-Bus loss is deliberately independent from service availability: a lost
// connection does not turn monitored services DOWN. It is exposed separately
// so Prometheus can distinguish "service is down" from "collector cannot see
// systemd".
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

func (r *Registry) Handler(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

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
}
