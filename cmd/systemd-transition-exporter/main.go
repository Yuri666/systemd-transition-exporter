package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/config"
	"github.com/Yuri666/systemd-transition-exporter/internal/engine"
	"github.com/Yuri666/systemd-transition-exporter/internal/metrics"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
	"github.com/Yuri666/systemd-transition-exporter/internal/recovery"
	"github.com/Yuri666/systemd-transition-exporter/internal/remote_write"
	"github.com/Yuri666/systemd-transition-exporter/internal/systemd"
	"github.com/Yuri666/systemd-transition-exporter/internal/wal"
)

func main() {
	configPath := flag.String("config", "/etc/systemd-transition-exporter/config.yaml", "configuration file")
	flag.Parse()
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	httpListener, err := net.Listen("tcp", cfg.Server.Listen)
	if err != nil {
		log.Fatalf("HTTP server: cannot listen on %s: %v", cfg.Server.Listen, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := engine.New()
	reg := metrics.New()
	var eventLog *wal.WAL
	if cfg.WAL.Enabled {
		eventLog, err = wal.Open(cfg.WAL.Directory, cfg.WAL.Fsync)
		if err != nil {
			_ = httpListener.Close()
			log.Fatal(err)
		}
		defer eventLog.Close()
		for _, state := range eventLog.States() {
			eng.RestoreState(state)
			reg.SetState(state.Service, state.Availability)
		}
	}

	persist := func(event model.Event) error {
		if eventLog != nil {
			if err := eventLog.Append(event); err != nil {
				return err
			}
		}
		return nil
	}

	walPath := filepath.Join(cfg.WAL.Directory, "events.jsonl")
	var durableEvents []model.Event
	if cfg.WAL.Enabled {
		if events, e := wal.ReadAll(walPath); e == nil {
			durableEvents = events
			for _, event := range events {
				eng.Replay(event)
				reg.Event(event)
			}
			if len(events) > 0 {
				log.Printf("replayed %d durable transition events from WAL", len(events))
			}
		} else if !os.IsNotExist(e) {
			_ = httpListener.Close()
			log.Fatalf("replay WAL: %v", e)
		}
	}

	startupTo := time.Now()
	slotStart := recovery.SlotStart(startupTo, cfg.RemoteWrite.RecoveryWindow)

	currentBootID, err := systemd.BootID()
	if err != nil {
		_ = httpListener.Close()
		log.Fatalf("read current boot id: %v", err)
	}
	bootChanged := false
	for _, service := range cfg.Services {
		if state, ok := eng.State(service); ok && state.BootID != "" && state.BootID != currentBootID {
			bootChanged = true
			break
		}
	}
	if bootChanged {
		bootTime, e := systemd.BootTime()
		if e != nil {
			_ = httpListener.Close()
			log.Fatalf("read current boot time: %v", e)
		}
		// A reboot older than the slot lies in an already closed interval, so
		// it must not be republished. The upcoming snapshot re-establishes the
		// current state instead.
		if bootTime.Before(slotStart) {
			log.Printf("host reboot at %s precedes recovery slot %s; adopting boot id without republishing downtime", bootTime.Format(time.RFC3339), slotStart.Format(time.RFC3339))
			eng.AdoptBootID(currentBootID)
		}
		for _, event := range eng.ApplyReboot(currentBootID, bootTime) {
			if e := persist(event); e != nil {
				_ = httpListener.Close()
				log.Fatalf("persist host reboot event: %v", e)
			}
			reg.Event(event)
			log.Printf("host reboot transition seq=%d service=%s state=%s timestamp_ms=%d", event.Sequence, event.Service, event.State, event.EventTimeUnixMS)
		}
		if eventLog != nil {
			for _, service := range cfg.Services {
				if state, ok := eng.State(service); ok {
					if e := eventLog.SaveState(state); e != nil {
						_ = httpListener.Close()
						log.Fatalf("persist post-reboot state: %v", e)
					}
				}
			}
		}
	}

	startupObservedFrom := startupTo.Add(-cfg.Systemd.StartupRecoveryInterval)
	if len(durableEvents) > 0 {
		newest := durableEvents[0].EventTimeUnixMS
		for _, event := range durableEvents[1:] {
			if event.EventTimeUnixMS > newest {
				newest = event.EventTimeUnixMS
			}
		}
		startupObservedFrom = time.UnixMilli(newest)
	}
	var startupFill []model.StateSample
	var startupRecovered []model.Event
	var startupSlotStart time.Time
	if windowStart, windowEnd, ok := recovery.RecoveryWindow(startupTo, cfg.RemoteWrite.RecoveryWindow); ok {
		startupSlotStart = windowStart
		reg.RecordRecovery(startupObservedFrom, windowStart, windowEnd)
		log.Printf("starting journal recovery slot from=%s to=%s", windowStart.Format(time.RFC3339Nano), windowEnd.Format(time.RFC3339Nano))
		if events, e := recovery.Recover(ctx, cfg.Services, windowStart, windowEnd); e != nil {
			log.Printf("startup journal recovery failed: %v", e)
		} else {
			imported := 0
			for _, recovered := range events {
				for _, event := range eng.ApplyRecovery(recovered) {
					if err := persist(event); err != nil {
						_ = httpListener.Close()
						log.Fatalf("persist startup recovery event: %v", err)
					}
					reg.Event(event)
					startupRecovered = append(startupRecovered, event)
					imported++
					if eventLog != nil {
						if state, ok := eng.State(event.Service); ok {
							if err := eventLog.SaveState(state); err != nil {
								_ = httpListener.Close()
								log.Fatalf("persist startup recovery state: %v", err)
							}
						}
					}
					log.Printf("startup recovered transition seq=%d service=%s state=%s timestamp_ms=%d source=%s", event.Sequence, event.Service, event.State, event.EventTimeUnixMS, event.Source)
				}
			}
			if fill, e := recovery.BuildStateFill(ctx, cfg.Services, windowStart, windowEnd, events, cfg.RemoteWrite.RecoveryFillInterval); e != nil {
				log.Printf("startup recovery fill failed: %v", e)
			} else {
				startupFill = fill
			}
			log.Printf("startup journal recovery completed: candidates=%d imported=%d fill_samples=%d", len(events), imported, len(startupFill))
		}
	}

	rw, err := remote_write.New(remote_write.Config{
		Enabled: cfg.RemoteWrite.Enabled, URL: cfg.RemoteWrite.URL,
		BatchSize: cfg.RemoteWrite.BatchSize, FlushInterval: cfg.RemoteWrite.FlushInterval,
		RetryInterval: cfg.RemoteWrite.RetryInterval, Timeout: cfg.RemoteWrite.Timeout,
		Checkpoint: cfg.RemoteWrite.Checkpoint, StateInterval: cfg.RemoteWrite.StateInterval,
		Labels: cfg.RemoteWrite.Labels,
	})
	if err != nil {
		_ = httpListener.Close()
		log.Fatal(err)
	}

	if rw != nil {
		reg.SetRemoteWriteStats(rw.Stats())
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					reg.SetRemoteWriteStats(rw.Stats())
				case <-ctx.Done():
					reg.SetRemoteWriteStats(rw.Stats())
					return
				}
			}
		}()
	}

	eventQueue := make(chan model.Event, 100000)
	stateQueue := make(chan model.ServiceState, 10000)
	if rw != nil {
		go func() {
			if cfg.WAL.Enabled {
				if events, e := wal.ReadAll(walPath); e == nil {
					split := 0
					if !startupSlotStart.IsZero() {
						for split < len(events) && events[split].EventTimeUnixMS < startupSlotStart.UnixMilli() {
							split++
						}
					}
					if e := rw.Send(ctx, events[:split]); e != nil && ctx.Err() == nil {
						log.Printf("remote_write WAL recovery stopped: %v", e)
					}
					if e := rw.SendRecoveredStates(ctx, startupFill); e != nil && ctx.Err() == nil {
						log.Printf("remote_write startup fill stopped: %v", e)
					}
					if e := rw.Send(ctx, events[split:]); e != nil && ctx.Err() == nil {
						log.Printf("remote_write recovery stopped: %v", e)
					}
				} else if !os.IsNotExist(e) {
					log.Printf("remote_write WAL scan: %v", e)
				}
			} else {
				if e := rw.SendRecoveredStates(ctx, startupFill); e != nil && ctx.Err() == nil {
					log.Printf("remote_write startup fill stopped: %v", e)
				}
				if e := rw.Send(ctx, startupRecovered); e != nil && ctx.Err() == nil {
					log.Printf("remote_write startup recovery stopped: %v", e)
				}
			}

			batch := make([]model.Event, 0, cfg.RemoteWrite.BatchSize)
			timer := time.NewTicker(cfg.RemoteWrite.FlushInterval)
			stateTimer := time.NewTicker(cfg.RemoteWrite.StateInterval)
			defer timer.Stop()
			defer stateTimer.Stop()
			flush := func() {
				if len(batch) == 0 {
					return
				}
				if e := rw.Send(ctx, batch); e != nil && ctx.Err() == nil {
					log.Printf("remote_write send failed: %v", e)
				} else {
					batch = batch[:0]
				}
			}
			drainEvents := func() {
				for {
					select {
					case e := <-eventQueue:
						batch = append(batch, e)
					default:
						return
					}
				}
			}
			sendQueuedStates := func() {
				for {
					select {
					case state := <-stateQueue:
						if e := rw.SendCurrentStates(ctx, []model.ServiceState{state}); e != nil && ctx.Err() == nil {
							log.Printf("remote_write startup state failed for %s: %v", state.Service, e)
						}
					default:
						return
					}
				}
			}

			for {
				select {
				case e := <-eventQueue:
					batch = append(batch, e)
					if len(batch) >= cfg.RemoteWrite.BatchSize {
						flush()
					}
				case state := <-stateQueue:
					if e := rw.SendCurrentStates(ctx, []model.ServiceState{state}); e != nil && ctx.Err() == nil {
						log.Printf("remote_write startup state failed for %s: %v", state.Service, e)
					}
				case <-timer.C:
					drainEvents()
					flush()
				case <-stateTimer.C:
					drainEvents()
					flush()
					sendQueuedStates()
					states := make([]model.ServiceState, 0, len(cfg.Services))
					for _, service := range cfg.Services {
						if st, ok := eng.State(service); ok {
							states = append(states, st)
						}
					}
					if e := rw.SendCurrentStates(ctx, states); e != nil && ctx.Err() == nil {
						log.Printf("remote_write state heartbeat failed: %v", e)
					}
				case <-ctx.Done():
					drainEvents()
					flush()
					return
				}
			}
		}()
	}

	persistEvent := func(event model.Event, enqueue bool) error {
		if eventLog != nil {
			if err := eventLog.Append(event); err != nil {
				return err
			}
		}
		if enqueue && rw != nil {
			select {
			case eventQueue <- event:
			default:
				log.Printf("remote_write queue full; event seq=%d remains durable in WAL", event.Sequence)
			}
		}
		return nil
	}
	persist = func(event model.Event) error { return persistEvent(event, true) }

	onSnapshot := func(s model.UnitSnapshot) error {
		for _, event := range eng.Apply(s) {
			if err := persist(event); err != nil {
				return err
			}
			reg.Event(event)
			log.Printf("transition seq=%d service=%s state=%s timestamp_ms=%d source=%s", event.Sequence, event.Service, event.State, event.EventTimeUnixMS, event.Source)
		}
		state := model.ServiceState{Service: s.Service, Availability: currentState(s.ActiveState), ActiveState: s.ActiveState, SubState: s.SubState, BootID: s.BootID}
		if existing, ok := eng.State(s.Service); ok {
			state = existing
		}
		if eventLog != nil {
			if err := eventLog.SaveState(state); err != nil {
				return err
			}
		}
		reg.SetState(s.Service, state.Availability)
		if rw != nil {
			select {
			case stateQueue <- state:
			default:
				log.Printf("remote_write state queue full; startup state for %s will be sent at next state_interval", s.Service)
			}
		}
		return nil
	}

	onRecovery := func(from, to time.Time) error {
		windowStart, windowEnd, ok := recovery.RecoveryWindow(to, cfg.RemoteWrite.RecoveryWindow)
		if !ok {
			return nil
		}
		reg.RecordRecovery(from, windowStart, windowEnd)
		count := 0
		events, err := recovery.Recover(ctx, cfg.Services, windowStart, windowEnd)
		if err != nil {
			return err
		}
		recovered := make([]model.Event, 0, len(events))
		for _, candidate := range events {
			for _, event := range eng.ApplyRecovery(candidate) {
				if err := persistEvent(event, false); err != nil {
					return err
				}
				reg.Event(event)
				recovered = append(recovered, event)
				count++
				if eventLog != nil {
					if state, ok := eng.State(event.Service); ok {
						if err := eventLog.SaveState(state); err != nil {
							return err
						}
					}
				}
				log.Printf("recovered transition seq=%d service=%s state=%s timestamp_ms=%d source=%s", event.Sequence, event.Service, event.State, event.EventTimeUnixMS, event.Source)
			}
		}

		if rw != nil {
			fill, err := recovery.BuildStateFill(ctx, cfg.Services, windowStart, windowEnd, events, cfg.RemoteWrite.RecoveryFillInterval)
			if err != nil {
				return err
			}
			// A rejected continuity sample must not discard the recovered
			// transitions: the transitions are the authoritative history.
			if err := rw.SendRecoveredStates(ctx, fill); err != nil {
				log.Printf("recovery state fill failed: %v", err)
			}
			if err := rw.Send(ctx, recovered); err != nil {
				return err
			}
			if len(fill) > 0 {
				log.Printf("recovery state fill slot=%s..%s interval=%s samples=%d", windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), cfg.RemoteWrite.RecoveryFillInterval, len(fill))
			}
		}
		log.Printf("journal recovery completed: slot=%s..%s candidates=%d imported=%d", windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), len(events), count)
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", reg.Handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if cfg.Server.Debug {
		mux.HandleFunc("/debug/dbus/disconnect", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if !systemd.DebugDisconnect() {
				http.Error(w, "D-Bus connection is not active", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
	}
	server := &http.Server{Handler: mux}
	go func() {
		log.Printf("listening on %s", cfg.Server.Listen)
		if err := server.Serve(httpListener); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server: %v", err)
		}
	}()
	go func() {
		err := systemd.RunResilient(ctx, cfg.Services, cfg.Systemd.ReconnectInterval, cfg.Systemd.ReconciliationInterval, onSnapshot, func(connected bool, at time.Time) {
			reg.SetDBusConnected(connected, at)
			if connected {
				log.Printf("systemd D-Bus connected")
			} else {
				log.Printf("systemd D-Bus disconnected")
			}
		}, func(err error) { log.Printf("systemd D-Bus error: %v", err) }, onRecovery)
		if err != nil && err != context.Canceled {
			log.Printf("systemd monitor stopped: %v", err)
			stop()
		}
	}()
	<-ctx.Done()
	_ = server.Shutdown(context.Background())
}

func currentState(active string) model.AvailabilityState {
	if active == "active" {
		return model.StateUp
	}
	return model.StateDown
}
