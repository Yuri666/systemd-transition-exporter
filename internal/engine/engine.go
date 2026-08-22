package engine

import (
	"sort"
	"sync"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type Engine struct {
	mu sync.Mutex
	states map[string]model.ServiceState
	seq    uint64
}

func New() *Engine { return &Engine{states: make(map[string]model.ServiceState)} }

func (e *Engine) State(service string) (model.ServiceState, bool) {
	e.mu.Lock(); defer e.mu.Unlock()
	s, ok := e.states[service]
	return s, ok
}

func (e *Engine) Replay(event model.Event) {
	e.mu.Lock(); defer e.mu.Unlock()
	if event.Sequence > e.seq { e.seq = event.Sequence }
	state := e.states[event.Service]
	if state.Service == "" { state.Service = event.Service }
	if event.EventTimeUnixMS < state.LastEventTimeUnixMS { return }
	state.Availability = event.State
	state.BootID = event.BootID
	state.LastEventTimeUnixMS = event.EventTimeUnixMS
	state.LastEventMonoUS = event.EventTimeMonotonicUS
	state.LastSequence = event.Sequence
	e.states[event.Service] = state
}

// Apply consumes a systemd snapshot. A candidate is emitted only when it is
// newer than both the corresponding systemd timestamp already observed and
// the last event timestamp restored/recovered by the engine. The second
// condition prevents journal-recovered events from being emitted again by the
// first D-Bus snapshot after startup/reconnect.
func (e *Engine) Apply(s model.UnitSnapshot) []model.Event {
	e.mu.Lock(); defer e.mu.Unlock()
	newState := availability(s.ActiveState)
	old, exists := e.states[s.Service]
	if !exists || old.BootID != s.BootID {
		e.states[s.Service] = model.ServiceState{Service: s.Service, Availability: newState, ActiveState: s.ActiveState, SubState: s.SubState, BootID: s.BootID, LastActiveEnterTimestampUS: s.ActiveEnterTimestampUS, LastActiveExitTimestampUS: s.ActiveExitTimestampUS, LastActiveEnterMonotonicUS: s.ActiveEnterTimestampMonotonicUS, LastActiveExitMonotonicUS: s.ActiveExitTimestampMonotonicUS, LastEventTimeUnixMS: old.LastEventTimeUnixMS, LastEventMonoUS: old.LastEventMonoUS, LastSequence: old.LastSequence}
		return nil
	}
	type candidate struct { state model.AvailabilityState; unixUS, monoUS uint64 }
	var candidates []candidate
	if s.ActiveExitTimestampUS > old.LastActiveExitTimestampUS && int64(s.ActiveExitTimestampUS/1000) > old.LastEventTimeUnixMS {
		candidates = append(candidates, candidate{model.StateDown, s.ActiveExitTimestampUS, s.ActiveExitTimestampMonotonicUS})
	}
	if s.ActiveEnterTimestampUS > old.LastActiveEnterTimestampUS && int64(s.ActiveEnterTimestampUS/1000) > old.LastEventTimeUnixMS {
		candidates = append(candidates, candidate{model.StateUp, s.ActiveEnterTimestampUS, s.ActiveEnterTimestampMonotonicUS})
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].unixUS < candidates[j].unixUS })
	events := make([]model.Event, 0, len(candidates))
	for _, c := range candidates {
		e.seq++
		events = append(events, model.Event{Sequence: e.seq, Service: s.Service, State: c.state, EventTimeUnixMS: int64(c.unixUS / 1000), EventTimeMonotonicUS: c.monoUS, BootID: s.BootID, Source: model.SourceSystemd, SystemdActiveState: s.ActiveState, SystemdSubState: s.SubState})
	}
	updated := model.ServiceState{Service: s.Service, Availability: newState, ActiveState: s.ActiveState, SubState: s.SubState, BootID: s.BootID, LastActiveEnterTimestampUS: s.ActiveEnterTimestampUS, LastActiveExitTimestampUS: s.ActiveExitTimestampUS, LastActiveEnterMonotonicUS: s.ActiveEnterTimestampMonotonicUS, LastActiveExitMonotonicUS: s.ActiveExitTimestampMonotonicUS, LastEventTimeUnixMS: old.LastEventTimeUnixMS, LastEventMonoUS: old.LastEventMonoUS, LastSequence: old.LastSequence}
	if len(events) > 0 { last := events[len(events)-1]; updated.LastEventTimeUnixMS = last.EventTimeUnixMS; updated.LastEventMonoUS = last.EventTimeMonotonicUS; updated.LastSequence = last.Sequence }
	e.states[s.Service] = updated
	return events
}

func (e *Engine) ApplyRecovery(event model.Event) []model.Event {
	e.mu.Lock(); defer e.mu.Unlock()
	state := e.states[event.Service]
	if state.Service != "" && event.EventTimeUnixMS <= state.LastEventTimeUnixMS { return nil }
	if state.Service == "" { state.Service = event.Service }
	e.seq++
	event.Sequence = e.seq
	state.Service = event.Service
	state.Availability = event.State
	state.BootID = event.BootID
	state.LastEventTimeUnixMS = event.EventTimeUnixMS
	state.LastSequence = event.Sequence
	e.states[event.Service] = state
	return []model.Event{event}
}

func (e *Engine) ApplyReboot(bootID string, eventTime time.Time) []model.Event {
	e.mu.Lock(); defer e.mu.Unlock()
	var events []model.Event
	for service, state := range e.states {
		if state.Availability != model.StateUp { continue }
		e.seq++
		event := model.Event{Sequence: e.seq, Service: service, State: model.StateDown, EventTimeUnixMS: eventTime.UnixMilli(), BootID: bootID, Source: model.SourceHostReboot, SystemdActiveState: "inactive", SystemdSubState: "dead"}
		events = append(events, event)
		state.Availability = model.StateDown; state.BootID = bootID; state.LastEventTimeUnixMS = event.EventTimeUnixMS; state.LastSequence = event.Sequence
		e.states[service] = state
	}
	return events
}

func (e *Engine) Sequence() uint64 { e.mu.Lock(); defer e.mu.Unlock(); return e.seq }

func availability(activeState string) model.AvailabilityState {
	if activeState == "active" { return model.StateUp }
	return model.StateDown
}
