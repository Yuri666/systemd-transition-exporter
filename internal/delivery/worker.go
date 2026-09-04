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
	SendCurrentStates(context.Context, []model.ServiceState) error
	LastSent() uint64
}

type RecoveryJob struct {
	Fill   []model.StateSample
	Events []model.Event
}

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
	BuildSlotFill  func(context.Context) ([]model.StateSample, error)
	OnDropped      func(string, int)
}

type command struct {
	event    *model.Event
	state    *model.ServiceState
	recovery *RecoveryJob
}

type Worker struct {
	cfg    Config
	sender Sender
	queue  chan command
}

func New(cfg Config, sender Sender) *Worker {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &Worker{
		cfg:    cfg,
		sender: sender,
		queue:  make(chan command, 100000),
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

func (w *Worker) EnqueueState(state model.ServiceState) bool {
	select {
	case w.queue <- command{state: &state}:
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
	var pendingClosings []time.Time
	var nextClosing time.Time
	var closingTimer *time.Timer
	var closingC <-chan time.Time
	if w.cfg.RecoveryWindow > recovery.SlotEndLead {
		now := time.Now()
		if due := recovery.DueSlotClosing(now, w.cfg.RecoveryWindow); !due.IsZero() {
			pendingClosings = append(pendingClosings, due)
		}
		nextClosing = recovery.NextSlotClosing(now, w.cfg.RecoveryWindow)
		d := time.Until(nextClosing)
		if d < 0 {
			d = 0
		}
		closingTimer = time.NewTimer(d)
		closingC = closingTimer.C
		defer closingTimer.Stop()
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
	sendCurrent := func(states []model.ServiceState) {
		if len(states) == 0 || pendingRecovery != nil || !flush() {
			return
		}
		if err := w.sender.SendCurrentStates(ctx, states); err != nil && ctx.Err() == nil {
			markDegraded()
			log.Printf("remote_write target=%s current state failed: %v", w.cfg.TargetID, err)
		}
	}
	heartbeat := func() {
		// Commands are produced in observation order. Do not let an internal
		// timer publish a current-time sample ahead of queued transitions or a
		// recovery job.
		if len(w.queue) > 0 || !flush() || pendingRecovery != nil {
			return
		}
		if !degradedSince.IsZero() {
			if time.Since(degradedSince) >= w.cfg.StateInterval && w.cfg.BuildSlotFill != nil {
				fill, err := w.cfg.BuildSlotFill(ctx)
				if err != nil {
					log.Printf("remote_write target=%s recovery republish build failed: %v", w.cfg.TargetID, err)
					return
				}
				if err := w.sender.SendRecoveredStates(ctx, fill); err != nil {
					log.Printf("remote_write target=%s recovery republish send failed: %v", w.cfg.TargetID, err)
					if !remote_write.IsPermanent(err) {
						return
					}
				} else {
					log.Printf("remote_write target=%s delivery recovered: republished samples=%d", w.cfg.TargetID, len(fill))
				}
			}
			degradedSince = time.Time{}
		}
		states := make([]model.ServiceState, 0, len(w.cfg.Services))
		for _, service := range w.cfg.Services {
			if state, ok := w.cfg.CurrentState(service); ok {
				states = append(states, state)
			}
		}
		sendCurrent(states)
	}
	sendClosing := func(at time.Time) bool {
		if at.IsZero() || pendingRecovery != nil || len(w.queue) > 0 || !flush() {
			return false
		}
		samples := slotClosingSamples(w.cfg.Services, w.cfg.CurrentState, at)
		if len(samples) == 0 {
			return true
		}
		if err := w.sender.SendRecoveredStates(ctx, samples); err != nil {
			if ctx.Err() == nil {
				markDegraded()
				log.Printf("remote_write target=%s slot closing sample failed: %v", w.cfg.TargetID, err)
			}
			return remote_write.IsPermanent(err)
		}
		log.Printf("remote_write target=%s slot closing sample at %s samples=%d", w.cfg.TargetID, at.Format(time.RFC3339Nano), len(samples))
		return true
	}
	tryClosings := func() {
		for len(pendingClosings) > 0 {
			if !sendClosing(pendingClosings[0]) {
				return
			}
			pendingClosings = pendingClosings[1:]
		}
	}
	enqueueClosing := func(at time.Time) {
		if at.IsZero() {
			return
		}
		if n := len(pendingClosings); n > 0 && pendingClosings[n-1].Equal(at) {
			return
		}
		pendingClosings = append(pendingClosings, at)
		if len(pendingClosings) > 2 {
			pendingClosings = pendingClosings[len(pendingClosings)-2:]
		}
	}

	for {
		if pendingRecovery != nil && sendRecovery(pendingRecovery) {
			pendingRecovery = nil
		}
		tryClosings()
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
			case cmd.state != nil:
				sendCurrent([]model.ServiceState{*cmd.state})
			}
		case <-flushTicker.C:
			flush()
		case <-stateTicker.C:
			heartbeat()
		case <-closingC:
			enqueueClosing(nextClosing)
			now := time.Now()
			for {
				nextClosing = recovery.SlotClosingTime(recovery.SlotStart(nextClosing, w.cfg.RecoveryWindow).Add(w.cfg.RecoveryWindow), w.cfg.RecoveryWindow)
				if now.Before(nextClosing) {
					break
				}
				enqueueClosing(nextClosing)
			}
			d := time.Until(nextClosing)
			if d < 0 {
				d = 0
			}
			closingTimer.Reset(d)
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
	}
	if err := w.sender.Send(ctx, pending[split:]); err != nil {
		if ctx.Err() == nil {
			log.Printf("remote_write target=%s startup recovery stopped: %v", w.cfg.TargetID, err)
		}
		return pending[split:], time.Now()
	}
	return nil, degradedSince
}

func slotClosingSamples(services []string, current func(string) (model.ServiceState, bool), at time.Time) []model.StateSample {
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
