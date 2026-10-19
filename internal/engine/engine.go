package engine

import (
	"sort"
	"sync"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type Engine struct {
	mu      sync.Mutex
	states  map[string]model.ServiceState
	seq     uint64
	delayUp map[string]struct{}
}

// DelayUpUntilReady holds the UP transition of each named service until
// ConfirmReady. The first observation of an already active unit is unaffected:
// a probe starts only from a transition seen while this process is running.
func (e *Engine) DelayUpUntilReady(services []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.delayUp = make(map[string]struct{}, len(services))
	for _, service := range services {
		e.delayUp[service] = struct{}{}
	}
}

func New() *Engine { return &Engine{states: make(map[string]model.ServiceState)} }

func (e *Engine) State(service string) (model.ServiceState, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.states[service]
	return s, ok
}

func (e *Engine) RestoreState(state model.ServiceState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if state.Service == "" {
		return
	}
	e.states[state.Service] = state
	if state.LastSequence > e.seq {
		e.seq = state.LastSequence
	}
}

func (e *Engine) Replay(event model.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if event.Sequence > e.seq {
		e.seq = event.Sequence
	}
	state := e.states[event.Service]
	if state.Service == "" {
		state.Service = event.Service
	}
	if event.EventTimeUnixMS < state.LastEventTimeUnixMS {
		return
	}
	state.Availability = event.State
	state.AwaitingSIP = false
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
	e.mu.Lock()
	defer e.mu.Unlock()
	observed := availability(s.ActiveState)
	old, exists := e.states[s.Service]
	if !exists || old.BootID != s.BootID {
		// First look at this boot. An already active unit counts as up; the
		// SIP probe waits for a later transition, which this snapshot is not.
		e.states[s.Service] = model.ServiceState{Service: s.Service, Availability: observed, ActiveState: s.ActiveState, SubState: s.SubState, BootID: s.BootID, LastActiveEnterTimestampUS: s.ActiveEnterTimestampUS, LastActiveExitTimestampUS: s.ActiveExitTimestampUS, LastActiveEnterMonotonicUS: s.ActiveEnterTimestampMonotonicUS, LastActiveExitMonotonicUS: s.ActiveExitTimestampMonotonicUS, LastEventTimeUnixMS: old.LastEventTimeUnixMS, LastEventMonoUS: old.LastEventMonoUS, LastSequence: old.LastSequence}
		return nil
	}
	_, delayUp := e.delayUp[s.Service]
	type candidate struct {
		state          model.AvailabilityState
		unixUS, monoUS uint64
	}
	var candidates []candidate
	if s.ActiveExitTimestampUS > old.LastActiveExitTimestampUS && int64(s.ActiveExitTimestampUS/1000) > old.LastEventTimeUnixMS {
		candidates = append(candidates, candidate{model.StateDown, s.ActiveExitTimestampUS, s.ActiveExitTimestampMonotonicUS})
	}
	if s.ActiveEnterTimestampUS > old.LastActiveEnterTimestampUS && int64(s.ActiveEnterTimestampUS/1000) > old.LastEventTimeUnixMS {
		candidates = append(candidates, candidate{model.StateUp, s.ActiveEnterTimestampUS, s.ActiveEnterTimestampMonotonicUS})
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].unixUS < candidates[j].unixUS })
	current := old.Availability
	pending := old.AwaitingSIP
	events := make([]model.Event, 0, len(candidates))
	for _, c := range candidates {
		if c.state == model.StateUp && delayUp {
			pending = true
			continue
		}
		if c.state == model.StateDown {
			// The matching UP was withheld, so the series is already down and
			// a stop before the first 200 does not add another transition.
			wasPending := pending
			pending = false
			if wasPending && current == model.StateDown {
				continue
			}
		}
		e.seq++
		events = append(events, model.Event{Sequence: e.seq, Service: s.Service, State: c.state, EventTimeUnixMS: int64(c.unixUS / 1000), EventTimeMonotonicUS: c.monoUS, BootID: s.BootID, Source: model.SourceSystemd, SystemdActiveState: s.ActiveState, SystemdSubState: s.SubState})
		current = c.state
	}
	updated := model.ServiceState{Service: s.Service, Availability: observed, ActiveState: s.ActiveState, SubState: s.SubState, BootID: s.BootID, LastActiveEnterTimestampUS: s.ActiveEnterTimestampUS, LastActiveExitTimestampUS: s.ActiveExitTimestampUS, LastActiveEnterMonotonicUS: s.ActiveEnterTimestampMonotonicUS, LastActiveExitMonotonicUS: s.ActiveExitTimestampMonotonicUS, LastEventTimeUnixMS: old.LastEventTimeUnixMS, LastEventMonoUS: old.LastEventMonoUS, LastSequence: old.LastSequence, AwaitingSIP: pending}
	if pending {
		updated.Availability = model.StateDown
	}
	if len(events) > 0 {
		last := events[len(events)-1]
		updated.LastEventTimeUnixMS = last.EventTimeUnixMS
		updated.LastEventMonoUS = last.EventTimeMonotonicUS
		updated.LastSequence = last.Sequence
	}
	e.states[s.Service] = updated
	return events
}

// ConfirmReady releases the withheld UP transition at the moment the SIP
// interface answered. A late answer, after the unit left active, is discarded.
func (e *Engine) ConfirmReady(service string, at time.Time) (model.Event, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	state, ok := e.states[service]
	if !ok || !state.AwaitingSIP {
		return model.Event{}, false
	}
	ts := at.UnixMilli()
	if ts <= state.LastEventTimeUnixMS {
		ts = state.LastEventTimeUnixMS + 1
	}
	e.seq++
	event := model.Event{
		Sequence:           e.seq,
		Service:            service,
		State:              model.StateUp,
		EventTimeUnixMS:    ts,
		BootID:             state.BootID,
		Source:             model.SourceSIP,
		SystemdActiveState: state.ActiveState,
		SystemdSubState:    state.SubState,
	}
	state.AwaitingSIP = false
	state.Availability = model.StateUp
	state.LastEventTimeUnixMS = event.EventTimeUnixMS
	state.LastSequence = event.Sequence
	e.states[service] = state
	return event, true
}

func (e *Engine) ApplyRecovery(event model.Event) []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.states[event.Service]
	if state.Service != "" && event.EventTimeUnixMS <= state.LastEventTimeUnixMS {
		return nil
	}
	if state.Service == "" {
		state.Service = event.Service
	}
	e.seq++
	event.Sequence = e.seq
	state.Service = event.Service
	state.Availability = event.State
	state.AwaitingSIP = false
	state.BootID = event.BootID
	state.LastEventTimeUnixMS = event.EventTimeUnixMS
	state.LastSequence = event.Sequence
	e.states[event.Service] = state
	return []model.Event{event}
}

// AdoptBootID records the current boot without emitting downtime. It is used
// when the reboot happened before the interval the exporter is allowed to
// republish, so the boot change must not be mistaken for a fresh transition.
func (e *Engine) AdoptBootID(bootID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for service, state := range e.states {
		state.BootID = bootID
		e.states[service] = state
	}
}

func (e *Engine) ApplyReboot(bootID string, eventTime time.Time) []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	var events []model.Event
	for service, state := range e.states {
		if state.Availability != model.StateUp {
			state.BootID = bootID
			e.states[service] = state
			continue
		}
		e.seq++
		event := model.Event{Sequence: e.seq, Service: service, State: model.StateDown, EventTimeUnixMS: eventTime.UnixMilli(), BootID: bootID, Source: model.SourceHostReboot, SystemdActiveState: "inactive", SystemdSubState: "dead"}
		events = append(events, event)
		state.Availability = model.StateDown
		state.BootID = bootID
		state.LastEventTimeUnixMS = event.EventTimeUnixMS
		state.LastSequence = event.Sequence
		e.states[service] = state
	}
	return events
}

func (e *Engine) Sequence() uint64 { e.mu.Lock(); defer e.mu.Unlock(); return e.seq }

func availability(activeState string) model.AvailabilityState {
	if activeState == "active" {
		return model.StateUp
	}
	return model.StateDown
}
