package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Yuri666/systemd-transition-exporter/internal/engine"
	"github.com/Yuri666/systemd-transition-exporter/internal/systemd"
)

var services = []string{"pcscf.service", "scscf.service", "icscf.service"}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	bootID, err := systemd.BootID()
	if err != nil { log.Fatal(err) }

	d, err := systemd.Connect(ctx)
	if err != nil { log.Fatal(err) }
	defer d.Close()

	eng := engine.New()
	mon := systemd.NewMonitor(d)

	// Subscribe before the initial snapshots to avoid the startup race where
	// a transition occurs between discovery and AddMatch.
	if err := mon.Subscribe(); err != nil { log.Fatal(err) }

	for _, service := range services {
		u, err := d.LoadUnit(service)
		if err != nil { log.Printf("load %s: %v", service, err); continue }
		mon.AddUnit(u)
		s, err := u.Snapshot(bootID)
		if err != nil { log.Printf("snapshot %s: %v", service, err); continue }
		for _, ev := range eng.Apply(s) { logEvent(ev) }
	}

	log.Printf("systemd transition monitor started; boot_id=%s", bootID)
	if err := mon.Run(ctx, func(u *systemd.Unit) error {
		s, err := u.Snapshot(bootID)
		if err != nil { return err }
		for _, ev := range eng.Apply(s) { logEvent(ev) }
		return nil
	}); err != nil && err != context.Canceled { log.Fatal(err) }
}

func logEvent(ev interface{ }) { log.Printf("transition: %+v", ev) }
