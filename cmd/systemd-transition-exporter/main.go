package main

import (
	"context"
	"flag"
	"log"
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
	if err != nil { log.Fatal(err) }
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := engine.New()
	reg := metrics.New()
	var eventLog *wal.WAL
	if cfg.WAL.Enabled {
		eventLog, err = wal.Open(cfg.WAL.Directory, cfg.WAL.Fsync)
		if err != nil { log.Fatal(err) }
		defer eventLog.Close()
	}

	persist := func(event model.Event) error {
		if eventLog != nil { if err := eventLog.Append(event); err != nil { return err } }
		return nil
	}

	walPath := filepath.Join(cfg.WAL.Directory, "events.jsonl")
	var durableEvents []model.Event
	if cfg.WAL.Enabled {
		if events, e := wal.ReadAll(walPath); e == nil {
			durableEvents = events
			for _, event := range events { eng.Replay(event); reg.Event(event) }
			if len(events) > 0 { log.Printf("replayed %d durable transition events from WAL", len(events)) }
		} else if !os.IsNotExist(e) { log.Fatalf("replay WAL: %v", e) }
	}

	// Recover transitions that happened while the exporter itself was stopped.
	// If the WAL has history, use its oldest event as the safe lower boundary so
	// every configured service is covered. With no history, use the configurable
	// startup recovery window.
	startupFrom := time.Now().Add(-cfg.Systemd.StartupRecoveryInterval)
	if len(durableEvents) > 0 {
		startupFrom = time.UnixMilli(durableEvents[0].EventTimeUnixMS)
		for _, event := range durableEvents[1:] {
			if t := time.UnixMilli(event.EventTimeUnixMS); t.Before(startupFrom) { startupFrom = t }
		}
	}
	startupTo := time.Now()
	if events, e := recovery.Recover(ctx, cfg.Services, startupFrom, startupTo); e != nil {
		log.Printf("startup journal recovery failed: %v", e)
	} else {
		imported := 0
		for _, recovered := range events {
			for _, event := range eng.ApplyRecovery(recovered) {
				if err := persist(event); err != nil { log.Fatalf("persist startup recovery event: %v", err) }
				reg.Event(event)
				imported++
				log.Printf("startup recovered transition seq=%d service=%s state=%s timestamp_ms=%d source=%s", event.Sequence, event.Service, event.State, event.EventTimeUnixMS, event.Source)
			}
		}
		if len(events) > 0 || imported > 0 { log.Printf("startup journal recovery completed: candidates=%d imported=%d", len(events), imported) }
	}

	rw, err := remote_write.New(remote_write.Config{
		Enabled: cfg.RemoteWrite.Enabled, URL: cfg.RemoteWrite.URL,
		BatchSize: cfg.RemoteWrite.BatchSize, FlushInterval: cfg.RemoteWrite.FlushInterval,
		RetryInterval: cfg.RemoteWrite.RetryInterval, Timeout: cfg.RemoteWrite.Timeout,
		Checkpoint: cfg.RemoteWrite.Checkpoint, StateInterval: cfg.RemoteWrite.StateInterval,
		Labels: cfg.RemoteWrite.Labels,
	})
	if err != nil { log.Fatal(err) }

	eventQueue := make(chan model.Event, 100000)
	stateQueue := make(chan model.ServiceState, 10000)
	if rw != nil {
		go func() {
			if cfg.WAL.Enabled {
				if events, e := wal.ReadAll(walPath); e == nil {
					if e := rw.Send(ctx, events); e != nil && ctx.Err() == nil { log.Printf("remote_write recovery stopped: %v", e) }
				} else if !os.IsNotExist(e) { log.Printf("remote_write WAL scan: %v", e) }
			}

			batch := make([]model.Event, 0, cfg.RemoteWrite.BatchSize)
			timer := time.NewTicker(cfg.RemoteWrite.FlushInterval)
			stateTimer := time.NewTicker(cfg.RemoteWrite.StateInterval)
			defer timer.Stop(); defer stateTimer.Stop()

			flush := func() {
				if len(batch) == 0 { return }
				if e := rw.Send(ctx, batch); e != nil && ctx.Err() == nil { log.Printf("remote_write send failed: %v", e) } else { batch = batch[:0] }
			}
			drainEvents := func() {
				for { select { case e := <-eventQueue: batch = append(batch, e); default: return } }
			}
			sendQueuedStates := func() {
				for {
					select {
					case state := <-stateQueue:
						if e := rw.SendCurrentStates(ctx, []model.ServiceState{state}); e != nil && ctx.Err() == nil { log.Printf("remote_write startup state failed for %s: %v", state.Service, e) }
					default: return
					}
				}
			}

			for {
				select {
				case e := <-eventQueue:
					batch = append(batch, e)
					if len(batch) >= cfg.RemoteWrite.BatchSize { flush() }
				case state := <-stateQueue:
					if e := rw.SendCurrentStates(ctx, []model.ServiceState{state}); e != nil && ctx.Err() == nil { log.Printf("remote_write startup state failed for %s: %v", state.Service, e) }
				case <-timer.C:
					drainEvents(); flush()
				case <-stateTimer.C:
					drainEvents(); flush(); sendQueuedStates()
					states := make([]model.ServiceState, 0, len(cfg.Services))
					for _, service := range cfg.Services { if st, ok := eng.State(service); ok { states = append(states, st) } }
					if e := rw.SendCurrentStates(ctx, states); e != nil && ctx.Err() == nil { log.Printf("remote_write state heartbeat failed: %v", e) }
				case <-ctx.Done():
					drainEvents(); flush(); return
				}
			}
		}()
	}

	persist = func(event model.Event) error {
		if eventLog != nil { if err := eventLog.Append(event); err != nil { return err } }
		if rw != nil { select { case eventQueue <- event: default: log.Printf("remote_write queue full; event seq=%d remains durable in WAL", event.Sequence) } }
		return nil
	}

	onSnapshot := func(s model.UnitSnapshot) error {
		for _, event := range eng.Apply(s) {
			if err := persist(event); err != nil { return err }
			reg.Event(event)
			log.Printf("transition seq=%d service=%s state=%s timestamp_ms=%d source=%s", event.Sequence, event.Service, event.State, event.EventTimeUnixMS, event.Source)
		}
		state := model.ServiceState{Service: s.Service, Availability: currentState(s.ActiveState), ActiveState: s.ActiveState, SubState: s.SubState, BootID: s.BootID}
		if existing, ok := eng.State(s.Service); ok { state = existing }
		reg.SetState(s.Service, state.Availability)
		if rw != nil { select { case stateQueue <- state: default: log.Printf("remote_write state queue full; startup state for %s will be sent at next state_interval", s.Service) } }
		return nil
	}

	onRecovery := func(from, to time.Time) error {
		events, err := recovery.Recover(ctx, cfg.Services, from, to)
		if err != nil { return err }
		count := 0
		for _, recovered := range events {
			for _, event := range eng.ApplyRecovery(recovered) {
				if err := persist(event); err != nil { return err }
				reg.Event(event); count++
				log.Printf("recovered transition seq=%d service=%s state=%s timestamp_ms=%d source=%s", event.Sequence, event.Service, event.State, event.EventTimeUnixMS, event.Source)
			}
		}
		log.Printf("journal recovery completed: candidates=%d imported=%d", len(events), count)
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", reg.Handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/debug/dbus/disconnect", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		if !systemd.DebugDisconnect() { http.Error(w, "D-Bus connection is not active", http.StatusServiceUnavailable); return }
		w.WriteHeader(http.StatusNoContent)
	})
	server := &http.Server{Addr: cfg.Server.Listen, Handler: mux}
	go func() { log.Printf("listening on %s", cfg.Server.Listen); if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed { log.Printf("HTTP server: %v", err) } }()
	go func() {
		err := systemd.RunResilient(ctx, cfg.Services, cfg.Systemd.ReconnectInterval, onSnapshot, func(connected bool, at time.Time) { reg.SetDBusConnected(connected, at); if connected { log.Printf("systemd D-Bus connected") } else { log.Printf("systemd D-Bus disconnected") } }, func(err error) { log.Printf("systemd D-Bus error: %v", err) }, onRecovery)
		if err != nil && err != context.Canceled { log.Printf("systemd monitor stopped: %v", err); stop() }
	}()
	<-ctx.Done(); _ = server.Shutdown(context.Background())
}

func currentState(active string) model.AvailabilityState { if active == "active" { return model.StateUp }; return model.StateDown }
