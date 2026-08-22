package metrics

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type Registry struct {
	mu     sync.RWMutex
	states map[string]model.AvailabilityState
	up     map[string]uint64
	down   map[string]uint64
	last   map[string]int64
}

func New() *Registry {
	return &Registry{states: map[string]model.AvailabilityState{}, up: map[string]uint64{}, down: map[string]uint64{}, last: map[string]int64{}}
}

func (r *Registry) SetState(service string, state model.AvailabilityState) {
	r.mu.Lock(); defer r.mu.Unlock(); r.states[service] = state
}

func (r *Registry) Event(e model.Event) {
	r.mu.Lock(); defer r.mu.Unlock()
	r.states[e.Service] = e.State
	r.last[e.Service] = e.EventTimeUnixMS
	if e.State == model.StateUp { r.up[e.Service]++ }
	if e.State == model.StateDown { r.down[e.Service]++ }
}

func (r *Registry) Handler(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock(); defer r.mu.RUnlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for service, state := range r.states {
		v := 0
		if state == model.StateUp { v = 1 }
		fmt.Fprintf(w, "systemd_service_state{service=%q} %d\n", service, v)
		fmt.Fprintf(w, "systemd_service_transitions_total{service=%q,state=\"up\"} %d\n", service, r.up[service])
		fmt.Fprintf(w, "systemd_service_transitions_total{service=%q,state=\"down\"} %d\n", service, r.down[service])
		fmt.Fprintf(w, "systemd_service_last_transition_timestamp_seconds{service=%q} %g\n", service, float64(r.last[service])/1000)
	}
}
