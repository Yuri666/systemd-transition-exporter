package delivery

import (
	"context"
	"log"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
	"github.com/Yuri666/systemd-transition-exporter/internal/recovery"
	"github.com/Yuri666/systemd-transition-exporter/internal/remote_write"
)

type Sender interface {
	Send(context.Context, []model.Event) error
	SendRecoveredStates(context.Context, []model.StateSample) error
	LastSent() uint64
}

type RecoveryJob struct {
	Fill   []model.StateSample
	Events []model.Event
}

// maxPendingStates bounds the availability backlog kept while a receiver is
// unreachable. One slot of heartbeat ticks for a realistic unit list stays far
// below it; the cap only matters for an outage much longer than a slot.
const maxPendingStates = 10000

type Config struct {
	TargetID       string
	BatchSize      int
	FlushInterval  time.Duration
	StateInterval  time.Duration
	RecoveryWindow time.Duration
	Services       []string
	StartupEvents  []model.Event
	StartupFill    []model.StateSample
	StartupSlot    time.Time
	CurrentState   func(string) (model.ServiceState, bool)
	OnDropped      func(string, int)
}

type command struct {
	event    *model.Event
	recovery *RecoveryJob
}

type Worker struct {
	cfg    Config
	sender Sender
	queue  chan command

	// published is the latest sample timestamp accepted per service, and
	// pendingStates holds the availability samples the receiver has not
	// accepted yet. Only the Run goroutine touches them.
	published     map[string]int64
	pendingStates []model.StateSample
}

func New(cfg Config, sender Sender) *Worker {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &Worker{
		cfg:       cfg,
		sender:    sender,
		queue:     make(chan command, 100000),
		published: make(map[string]int64),
	}
}

func (w *Worker) TargetID() string { return w.cfg.TargetID }

func (w *Worker) EnqueueEvent(event model.Event) bool {
	select {
	case w.queue <- command{event: &event}:
		return true
	default:
		return false
	}
}

func (w *Worker) EnqueueRecovery(fill []model.StateSample, events []model.Event) bool {
	job := &RecoveryJob{
		Fill:   append([]model.StateSample(nil), fill...),
		Events: append([]model.Event(nil), events...),
	}
	select {
	case w.queue <- command{recovery: job}:
		return true
	default:
		return false
	}
}

func (w *Worker) Run(ctx context.Context) {
	flushTicker := time.NewTicker(w.cfg.FlushInterval)
	stateTicker := time.NewTicker(w.cfg.StateInterval)
	defer flushTicker.Stop()
	defer stateTicker.Stop()

	batch, degradedSince := w.startup(ctx)
	var pendingRecovery *RecoveryJob
	var pendingSlotSamples []time.Time
	var nextOpening, nextClosing time.Time
	var slotTimer *time.Timer
	var slotC <-chan time.Time
	if w.cfg.RecoveryWindow > recovery.SlotEndLead {
		now := time.Now()
		size := w.cfg.RecoveryWindow
		if due := recovery.DueSlotOpening(now, size); !due.IsZero() {
			pendingSlotSamples = append(pendingSlotSamples, due)
		}
		if due := recovery.DueSlotClosing(now, size); !due.IsZero() {
			pendingSlotSamples = append(pendingSlotSamples, due)
		}
		nextOpening = recovery.NextSlotOpening(now, size)
		nextClosing = recovery.NextSlotClosing(now, size)
		nextDue := earlierTime(nextOpening, nextClosing)
		d := time.Until(nextDue)
		if d < 0 {
			d = 0
		}
		slotTimer = time.NewTimer(d)
		slotC = slotTimer.C
		defer slotTimer.Stop()
	}

	markDegraded := func() {
		if degradedSince.IsZero() {
			degradedSince = time.Now()
		}
	}
	drop := func(n int, err error) {
		log.Printf("remote_write target=%s rejected %d transition events; dropping them from the send queue: %v", w.cfg.TargetID, n, err)
		if w.cfg.OnDropped != nil {
			w.cfg.OnDropped(w.cfg.TargetID, n)
		}
	}
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		err := w.sender.Send(ctx, batch)
		if err == nil {
			batch = batch[:0]
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		markDegraded()
		if remote_write.IsPermanent(err) {
			drop(len(batch), err)
			batch = batch[:0]
			return true
		}
		log.Printf("remote_write target=%s send failed: %v", w.cfg.TargetID, err)
		return false
	}
	sendRecovery := func(job *RecoveryJob) bool {
		if !flush() {
			return false
		}
		if err := w.sender.SendRecoveredStates(ctx, job.Fill); err != nil {
			if ctx.Err() == nil {
				markDegraded()
				log.Printf("remote_write target=%s recovery fill failed: %v", w.cfg.TargetID, err)
			}
			if !remote_write.IsPermanent(err) {
				return false
			}
		}
		if err := w.sender.Send(ctx, job.Events); err != nil {
			if ctx.Err() == nil {
				markDegraded()
				if remote_write.IsPermanent(err) {
					drop(len(job.Events), err)
					return true
				}
				log.Printf("remote_write target=%s recovered transitions failed: %v", w.cfg.TargetID, err)
			}
			return false
		}
		return true
	}
	// deliverStates sends the availability backlog. Transitions go first: they
	// are older by definition, so a receiver sees the history that explains a
	// state before the state itself. While the send fails the samples stay
	// buffered, which is what turns a receiver outage into a gap the exporter
	// can still fill once the receiver answers again.
	deliverStates := func() bool {
		if len(w.pendingStates) == 0 {
			return true
		}
		if pendingRecovery != nil || len(w.queue) > 0 || !flush() {
			return false
		}
		samples := w.dropStale(w.pendingStates)
		if len(samples) > 0 {
			if err := w.sender.SendRecoveredStates(ctx, samples); err != nil {
				if ctx.Err() == nil {
					// One line per outage. The recovery line below closes it,
					// and every slot edge in between reports what is buffered.
					if degradedSince.IsZero() {
						log.Printf("remote_write target=%s state samples failed; buffering them until the receiver answers: %v", w.cfg.TargetID, err)
					}
					markDegraded()
				}
				if !remote_write.IsPermanent(err) {
					return false
				}
			} else {
				w.noteSamples(samples)
				if !degradedSince.IsZero() {
					log.Printf("remote_write target=%s delivery recovered after %s: replayed buffered samples=%d", w.cfg.TargetID, time.Since(degradedSince).Truncate(time.Second), len(samples))
					degradedSince = time.Time{}
				}
			}
		}
		w.pendingStates = w.pendingStates[:0]
		return true
	}
	recordStates := func(samples []model.StateSample) bool {
		// A sample describes the state the engine holds when it is built, so
		// it stays valid however late it is delivered. While transitions are
		// still queued the engine is behind the units, and the sample would
		// instead record a state the moment no longer had.
		if len(samples) == 0 || pendingRecovery != nil || len(w.queue) > 0 {
			return false
		}
		w.bufferStates(samples, time.Now())
		return true
	}
	heartbeat := func() {
		if recordStates(slotStateSamples(w.cfg.Services, w.cfg.CurrentState, time.Now())) {
			deliverStates()
		}
	}
	sendSlotSample := func(at time.Time) bool {
		if at.IsZero() {
			return true
		}
		samples := slotStateSamples(w.cfg.Services, w.cfg.CurrentState, at)
		if len(samples) == 0 {
			return true
		}
		if !recordStates(samples) {
			return false
		}
		kind := "closing"
		if recovery.SlotOpeningTime(recovery.SlotStart(at, w.cfg.RecoveryWindow)).Equal(at) {
			kind = "opening"
		}
		if deliverStates() {
			log.Printf("remote_write target=%s slot %s sample at %s samples=%d", w.cfg.TargetID, kind, at.Format(time.RFC3339Nano), len(samples))
		} else {
			log.Printf("remote_write target=%s slot %s sample at %s buffered until delivery recovers samples=%d", w.cfg.TargetID, kind, at.Format(time.RFC3339Nano), len(samples))
		}
		return true
	}
	trySlotSamples := func() {
		for len(pendingSlotSamples) > 0 {
			if !sendSlotSample(pendingSlotSamples[0]) {
				return
			}
			pendingSlotSamples = pendingSlotSamples[1:]
		}
	}
	enqueueSlotSample := func(at time.Time) {
		if at.IsZero() {
			return
		}
		if n := len(pendingSlotSamples); n > 0 && pendingSlotSamples[n-1].Equal(at) {
			return
		}
		pendingSlotSamples = append(pendingSlotSamples, at)
		if len(pendingSlotSamples) > 4 {
			pendingSlotSamples = pendingSlotSamples[len(pendingSlotSamples)-4:]
		}
	}
	advancePast := func(fired time.Time) {
		size := w.cfg.RecoveryWindow
		if fired.Equal(nextOpening) {
			nextOpening = recovery.SlotOpeningTime(recovery.SlotStart(nextOpening, size).Add(size))
			return
		}
		nextClosing = recovery.SlotClosingTime(recovery.SlotStart(nextClosing, size).Add(size), size)
	}

	for {
		if pendingRecovery != nil && sendRecovery(pendingRecovery) {
			pendingRecovery = nil
		}
		trySlotSamples()
		select {
		case cmd := <-w.queue:
			switch {
			case cmd.event != nil:
				batch = append(batch, *cmd.event)
				if len(batch) >= w.cfg.BatchSize {
					flush()
				}
			case cmd.recovery != nil:
				if pendingRecovery == nil {
					pendingRecovery = cmd.recovery
				} else {
					pendingRecovery.Fill = append(pendingRecovery.Fill, cmd.recovery.Fill...)
					pendingRecovery.Events = append(pendingRecovery.Events, cmd.recovery.Events...)
				}
			}
		case <-flushTicker.C:
			flush()
		case <-stateTicker.C:
			heartbeat()
		case <-slotC:
			fired := earlierTime(nextOpening, nextClosing)
			enqueueSlotSample(fired)
			advancePast(fired)
			now := time.Now()
			for {
				n := earlierTime(nextOpening, nextClosing)
				if now.Before(n) {
					d := time.Until(n)
					if d < 0 {
						d = 0
					}
					slotTimer.Reset(d)
					break
				}
				enqueueSlotSample(n)
				advancePast(n)
			}
		case <-ctx.Done():
			flush()
			return
		}
	}
}

func (w *Worker) startup(ctx context.Context) ([]model.Event, time.Time) {
	var degradedSince time.Time
	lastSent := w.sender.LastSent()
	pending := make([]model.Event, 0, len(w.cfg.StartupEvents))
	for _, event := range w.cfg.StartupEvents {
		if event.Sequence > lastSent {
			pending = append(pending, event)
		}
	}
	split := 0
	if !w.cfg.StartupSlot.IsZero() {
		for split < len(pending) && pending[split].EventTimeUnixMS < w.cfg.StartupSlot.UnixMilli() {
			split++
		}
	}
	if err := w.sender.Send(ctx, pending[:split]); err != nil {
		if ctx.Err() == nil {
			log.Printf("remote_write target=%s WAL recovery stopped: %v", w.cfg.TargetID, err)
		}
		return pending, time.Now()
	}
	if err := w.sender.SendRecoveredStates(ctx, w.cfg.StartupFill); err != nil {
		if ctx.Err() == nil {
			log.Printf("remote_write target=%s startup fill stopped: %v", w.cfg.TargetID, err)
			degradedSince = time.Now()
		}
		if !remote_write.IsPermanent(err) {
			w.bufferStates(w.cfg.StartupFill, time.Now())
		}
	}
	if err := w.sender.Send(ctx, pending[split:]); err != nil {
		if ctx.Err() == nil {
			log.Printf("remote_write target=%s startup recovery stopped: %v", w.cfg.TargetID, err)
		}
		return pending[split:], time.Now()
	}
	return nil, degradedSince
}

// bufferStates keeps availability samples a receiver has not accepted yet, so
// a delivery outage can be filled in afterwards instead of leaving a hole in
// the series.
func (w *Worker) bufferStates(samples []model.StateSample, now time.Time) {
	if len(samples) == 0 {
		return
	}
	w.pendingStates = append(w.pendingStates, samples...)
	w.pruneStates(now)
}

// pruneStates keeps the backlog inside the current slot. A receiver accepts
// out-of-order samples only within its configured window, and the exporter
// never writes into a slot that has already closed.
func (w *Worker) pruneStates(now time.Time) {
	if w.cfg.RecoveryWindow > 0 {
		floor := recovery.SlotStart(now, w.cfg.RecoveryWindow).UnixMilli()
		kept := w.pendingStates[:0]
		for _, sample := range w.pendingStates {
			if sample.TimestampUnixMS >= floor {
				kept = append(kept, sample)
			}
		}
		w.pendingStates = kept
	}
	if excess := len(w.pendingStates) - maxPendingStates; excess > 0 {
		w.pendingStates = append(w.pendingStates[:0], w.pendingStates[excess:]...)
	}
}

// dropStale removes samples that repeat a moment already published for the
// same series. Transitions are deliberately not part of this bookkeeping: a
// buffered sample carries the state observed at its own timestamp, so it
// remains true history even when a later transition reached the receiver
// first, and the receiver's out-of-order window is what accepts it.
func (w *Worker) dropStale(samples []model.StateSample) []model.StateSample {
	out := make([]model.StateSample, 0, len(samples))
	for _, sample := range samples {
		if last, ok := w.published[sample.Service]; ok && sample.TimestampUnixMS <= last {
			continue
		}
		out = append(out, sample)
	}
	return out
}

func (w *Worker) noteSamples(samples []model.StateSample) {
	for _, sample := range samples {
		w.notePublished(sample.Service, sample.TimestampUnixMS)
	}
}

func (w *Worker) notePublished(service string, timestampUnixMS int64) {
	if last, ok := w.published[service]; !ok || timestampUnixMS > last {
		w.published[service] = timestampUnixMS
	}
}

func slotStateSamples(services []string, current func(string) (model.ServiceState, bool), at time.Time) []model.StateSample {
	if current == nil || at.IsZero() {
		return nil
	}
	ts := at.UnixMilli()
	out := make([]model.StateSample, 0, len(services))
	for _, service := range services {
		state, ok := current(service)
		if !ok {
			continue
		}
		out = append(out, model.StateSample{Service: service, State: state.Availability, TimestampUnixMS: ts})
	}
	return out
}

func earlierTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
