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

func New() *Engine {
	return &Engine{states: make(map[string]model.ServiceState)}
}

func (e *Engine) State(service string) (model.ServiceState, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.states[service]
	return s, ok
}

// Apply consumes a systemd snapshot. The first snapshot initializes state and
// does not emit a transition. Later snapshots detect every newly observed
// ActiveEnter/ActiveExit timestamp, even when several transitions occurred
// between two observations.
func (e *Engine) Apply(s model.UnitSnapshot) []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	newState := availability(s.ActiveState)
	old, exists := e.states[s.Service]

	if !exists || old.BootID != s.BootID {
		e.states[s.Service] = model.ServiceState{
			Service: s.Service, Availability: newState,
			ActiveState: s.ActiveState, SubState: s.SubState, BootID: s.BootID,
			LastActiveEnterTimestampUS: s.ActiveEnterTimestampUS,
			LastActiveExitTimestampUS: s.ActiveExitTimestampUS,
			LastActiveEnterMonotonicUS: s.ActiveEnterTimestampMonotonicUS,
			LastActiveExitMonotonicUS: s.ActiveExitTimestampMonotonicUS,
		}
		return nil
	}

	type candidate struct {
		state model.AvailabilityState
		unixUS uint64
		monoUS uint64
	}
	var candidates []candidate

	if s.ActiveExitTimestampUS > old.LastActiveExitTimestampUS {
		candidates = append(candidates, candidate{model.StateDown, s.ActiveExitTimestampUS, s.ActiveExitTimestampMonotonicUS})
	}
	if s.ActiveEnterTimestampUS > old.LastActiveEnterTimestampUS {
		candidates = append(candidates, candidate{model.StateUp, s.ActiveEnterTimestampUS, s.ActiveEnterTimestampMonotonicUS})
	}

	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].unixUS < candidates[j].unixUS })

	events := make([]model.Event, 0, len(candidates))
	for _, c := range candidates {
		e.seq++
		event := model.Event{
			Sequence: e.seq,
			Service: s.Service,
			State: c.state,
			EventTimeUnixMS: int64(c.unixUS / 1000),
			EventTimeMonotonicUS: c.monoUS,
			BootID: s.BootID,
			Source: model.SourceSystemd,
			SystemdActiveState: s.ActiveState,
			SystemdSubState: s.SubState,
		}
		events = append(events, event)
	}

	e.states[s.Service] = model.ServiceState{
		Service: s.Service, Availability: newState,
		ActiveState: s.ActiveState, SubState: s.SubState, BootID: s.BootID,
		LastActiveEnterTimestampUS: s.ActiveEnterTimestampUS,
		LastActiveExitTimestampUS: s.ActiveExitTimestampUS,
		LastActiveEnterMonotonicUS: s.ActiveEnterTimestampMonotonicUS,
		LastActiveExitMonotonicUS: s.ActiveExitTimestampMonotonicUS,
	}
	if len(events) > 0 {
		last := events[len(events)-1]
		e.states[s.Service].LastEventTimeUnixMS = last.EventTimeUnixMS
		e.states[s.Service].LastEventMonoUS = last.EventTimeMonotonicUS
		e.states[s.Service].LastSequence = last.Sequence
	}
	return events
}

func (e *Engine) ApplyReboot(bootID string, eventTime time.Time) []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	var events []model.Event
	for service, state := range e.states {
		if state.Availability != model.StateUp {
			continue
		}
		e.seq++
		event := model.Event{
			Sequence: e.seq, Service: service, State: model.StateDown,
			EventTimeUnixMS: eventTime.UnixMilli(), BootID: bootID,
			Source: model.SourceHostReboot,
			SystemdActiveState: "inactive", SystemdSubState: "dead",
		}
		events = append(events, event)
		state.Availability = model.StateDown
		state.BootID = bootID
		state.LastEventTimeUnixMS = event.EventTimeUnixMS
		state.LastSequence = event.Sequence
		e.states[service] = state
	}
	return events
}

func availability(activeState string) model.AvailabilityState {
	switch activeState {
	case "active", "activating":
		return model.StateUp
	case "inactive", "failed", "deactivating":
		return model.StateDown
	default:
		return model.StateUnknown
	}
}
