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
	"github.com/Yuri666/systemd-transition-exporter/internal/delivery"
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

	deliveryStartupEvents := startupRecovered
	if cfg.WAL.Enabled {
		events, err := wal.ReadAll(walPath)
		if err != nil && !os.IsNotExist(err) {
			_ = httpListener.Close()
			log.Fatalf("remote_write WAL scan: %v", err)
		}
		deliveryStartupEvents = events
	}

	type targetRuntime struct {
		sender *remote_write.Sender
		worker *delivery.Worker
	}
	targets := make([]targetRuntime, 0, len(cfg.RemoteWrite.Targets))
	for _, target := range cfg.RemoteWrite.Targets {
		if err := remote_write.MigrateCheckpoint(target.LegacyCheckpoint, target.Checkpoint); err != nil {
			_ = httpListener.Close()
			log.Fatalf("remote_write target=%s checkpoint migration: %v", target.ID, err)
		}
		sender, err := remote_write.New(remote_write.Config{
			Enabled: true, URL: target.URL,
			BatchSize: cfg.RemoteWrite.BatchSize, FlushInterval: cfg.RemoteWrite.FlushInterval,
			RetryInterval: cfg.RemoteWrite.RetryInterval, Timeout: cfg.RemoteWrite.Timeout,
			Checkpoint: target.Checkpoint, StateInterval: cfg.RemoteWrite.StateInterval,
			Labels: cfg.RemoteWrite.Labels,
		})
		if err != nil {
			_ = httpListener.Close()
			log.Fatalf("remote_write target=%s: %v", target.ID, err)
		}
		worker := delivery.New(delivery.Config{
			TargetID:      target.ID,
			BatchSize:     cfg.RemoteWrite.BatchSize,
			FlushInterval: cfg.RemoteWrite.FlushInterval,
			StateInterval: cfg.RemoteWrite.StateInterval,
			Services:      cfg.Services,
			StartupEvents: deliveryStartupEvents,
			StartupFill:   startupFill,
			StartupSlot:   startupSlotStart,
			CurrentState:  eng.State,
			BuildSlotFill: func(ctx context.Context) ([]model.StateSample, error) {
				windowStart, windowEnd, ok := recovery.RecoveryWindow(time.Now(), cfg.RemoteWrite.RecoveryWindow)
				if !ok {
					return nil, nil
				}
				events, err := recovery.Recover(ctx, cfg.Services, windowStart, windowEnd)
				if err != nil {
					return nil, err
				}
				return recovery.BuildStateFill(ctx, cfg.Services, windowStart, windowEnd, events, cfg.RemoteWrite.RecoveryFillInterval)
			},
			OnDropped: reg.AddDroppedEvents,
		}, sender)
		targets = append(targets, targetRuntime{sender: sender, worker: worker})
		reg.SetRemoteWriteStats(target.ID, sender.Stats())
		log.Printf("remote_write target=%s url=%s checkpoint=%s", target.ID, target.URL, target.Checkpoint)
		go worker.Run(ctx)
	}
	if len(targets) > 0 {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			update := func() {
				for _, target := range targets {
					reg.SetRemoteWriteStats(target.worker.TargetID(), target.sender.Stats())
				}
			}
			for {
				select {
				case <-ticker.C:
					update()
				case <-ctx.Done():
					update()
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
		if enqueue {
			for _, target := range targets {
				if !target.worker.EnqueueEvent(event) {
					log.Printf("remote_write target=%s queue full; event seq=%d remains durable in WAL", target.worker.TargetID(), event.Sequence)
				}
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
		for _, target := range targets {
			if !target.worker.EnqueueState(state) {
				log.Printf("remote_write target=%s state queue full; state for %s will be sent at next state_interval", target.worker.TargetID(), s.Service)
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

		if len(targets) > 0 {
			fill, err := recovery.BuildStateFill(ctx, cfg.Services, windowStart, windowEnd, events, cfg.RemoteWrite.RecoveryFillInterval)
			if err != nil {
				return err
			}
			for _, target := range targets {
				if !target.worker.EnqueueRecovery(fill, recovered) {
					log.Printf("remote_write target=%s queue full; recovery remains durable in WAL", target.worker.TargetID())
				}
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
