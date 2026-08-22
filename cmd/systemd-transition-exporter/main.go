package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Yuri666/systemd-transition-exporter/internal/config"
	"github.com/Yuri666/systemd-transition-exporter/internal/engine"
	"github.com/Yuri666/systemd-transition-exporter/internal/metrics"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
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

	bootID, err := systemd.BootID()
	if err != nil { log.Fatal(err) }
	dbusConn, err := systemd.Connect(ctx)
	if err != nil { log.Fatal(err) }
	defer dbusConn.Close()

	eng := engine.New()
	reg := metrics.New()
	var eventLog *wal.WAL
	if cfg.WAL.Enabled {
		eventLog, err = wal.Open(cfg.WAL.Directory, cfg.WAL.Fsync)
		if err != nil { log.Fatal(err) }
		defer eventLog.Close()
	}

	monitor := systemd.NewMonitor(dbusConn)
	// Subscribe before discovery/snapshots to avoid the startup race.
	if err := monitor.Subscribe(); err != nil { log.Fatal(err) }

	for _, service := range cfg.Services {
		u, err := dbusConn.LoadUnit(service)
		if err != nil { log.Printf("load %s: %v", service, err); continue }
		monitor.AddUnit(u)
		s, err := u.Snapshot(bootID)
		if err != nil { log.Printf("snapshot %s: %v", service, err); continue }
		reg.SetState(service, currentState(s.ActiveState))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", reg.Handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	server := &http.Server{Addr: cfg.Server.Listen, Handler: mux}
	go func() {
		log.Printf("listening on %s", cfg.Server.Listen)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed { log.Printf("HTTP server: %v", err) }
	}()

	go func() {
		err := monitor.Run(ctx, func(u *systemd.Unit) error {
			s, err := u.Snapshot(bootID)
			if err != nil { return err }
			reg.SetState(s.Service, currentState(s.ActiveState))
			for _, event := range eng.Apply(s) {
				if eventLog != nil {
					if err := eventLog.Append(event); err != nil { return err }
				}
				reg.Event(event)
				log.Printf("transition seq=%d service=%s state=%s timestamp_ms=%d", event.Sequence, event.Service, event.State, event.EventTimeUnixMS)
			}
			return nil
		})
		if err != nil && err != context.Canceled { log.Printf("systemd monitor stopped: %v", err); stop() }
	}()

	<-ctx.Done()
	_ = server.Shutdown(context.Background())
}

func currentState(active string) model.AvailabilityState {
	switch active {
	case "active", "activating": return model.StateUp
	case "inactive", "failed", "deactivating": return model.StateDown
	default: return model.StateUnknown
	}
}
