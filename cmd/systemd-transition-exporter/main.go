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
	"github.com/Yuri666/systemd-transition-exporter/internal/systemd"
	"github.com/Yuri666/systemd-transition-exporter/internal/wal"
)

func main() {
	configPath := flag.String("config", "/etc/systemd-transition-exporter/config.yaml", "configuration file")
	flag.Parse()
	cfg, err := config.Load(*configPath); if err != nil { log.Fatal(err) }
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM); defer stop()

	eng := engine.New()
	reg := metrics.New()
	var eventLog *wal.WAL
	if cfg.WAL.Enabled {
		eventLog, err = wal.Open(cfg.WAL.Directory, cfg.WAL.Fsync); if err != nil { log.Fatal(err) }
		defer eventLog.Close()
		walPath := filepath.Join(cfg.WAL.Directory, "events.jsonl")
		if events, e := wal.ReadAll(walPath); e == nil {
			for _, event := range events { eng.Replay(event); reg.Event(event) }
			if len(events) > 0 { log.Printf("replayed %d durable transition events from WAL", len(events)) }
		} else if !os.IsNotExist(e) { log.Fatalf("replay WAL: %v", e) }
	}

	persist := func(event model.Event) error { if eventLog != nil { return eventLog.Append(event) }; return nil }
	onSnapshot := func(s model.UnitSnapshot) error {
		for _, event := range eng.Apply(s) {
			if err := persist(event); err != nil { return err }
			reg.Event(event)
			log.Printf("transition seq=%d service=%s state=%s timestamp_ms=%d source=%s", event.Sequence, event.Service, event.State, event.EventTimeUnixMS, event.Source)
		}
		reg.SetState(s.Service, currentState(s.ActiveState)); return nil
	}
	onRecovery := func(from, to time.Time) error {
		events, err := recovery.Recover(ctx, cfg.Services, from, to); if err != nil { return err }
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

	mux := http.NewServeMux(); mux.HandleFunc("/metrics", reg.Handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	server := &http.Server{Addr: cfg.Server.Listen, Handler: mux}
	go func() { log.Printf("listening on %s", cfg.Server.Listen); if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed { log.Printf("HTTP server: %v", err) } }()
	go func() {
		err := systemd.RunResilient(ctx, cfg.Services, cfg.Systemd.ReconnectInterval, onSnapshot,
			func(connected bool, at time.Time) { reg.SetDBusConnected(connected, at); if connected { log.Printf("systemd D-Bus connected") } else { log.Printf("systemd D-Bus disconnected") } },
			func(err error) { log.Printf("systemd D-Bus error: %v", err) }, onRecovery)
		if err != nil && err != context.Canceled { log.Printf("systemd monitor stopped: %v", err); stop() }
	}()
	<-ctx.Done(); _ = server.Shutdown(context.Background())
}

func currentState(active string) model.AvailabilityState { if active == "active" { return model.StateUp }; return model.StateDown }
