package systemd

import (
	"context"
	"fmt"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

// ReconnectLoop coordinates retries independently from the D-Bus implementation.
type ReconnectLoop struct { interval time.Duration }
func NewReconnectLoop(interval time.Duration) *ReconnectLoop { if interval <= 0 { interval=time.Second }; return &ReconnectLoop{interval:interval} }
func (r *ReconnectLoop) Run(ctx context.Context, connectFn func(context.Context) error) error {
	for {
		err := connectFn(ctx)
		if ctx.Err()!=nil { return ctx.Err() }
		if err==nil { return nil }
		t:=time.NewTimer(r.interval)
		select { case <-ctx.Done(): t.Stop(); return ctx.Err(); case <-t.C: }
	}
}

// RunResilient owns one complete system-bus connection at a time. After a
// disconnect it creates a new connection, subscribes first, then reloads and
// reconciles every configured unit. The last known service state is untouched
// while the connection is unavailable.
func RunResilient(ctx context.Context, services []string, reconnectInterval time.Duration, onSnapshot func(model.UnitSnapshot) error) error {
	loop:=NewReconnectLoop(reconnectInterval)
	return loop.Run(ctx, func(ctx context.Context) error { return runConnection(ctx, services, onSnapshot) })
}

func runConnection(ctx context.Context, services []string, onSnapshot func(model.UnitSnapshot) error) error {
	d, err:=Connect(ctx); if err!=nil { return err }; defer d.Close()
	bootID, err:=BootID(); if err!=nil { return err }
	monitor:=NewMonitor(d)
	// Subscribe before discovery/reconciliation to close the startup race.
	if err:=monitor.Subscribe(); err!=nil { return err }
	for _, service:=range services {
		u, err:=d.LoadUnit(service); if err!=nil { return fmt.Errorf("load %s: %w",service,err) }
		monitor.AddUnit(u)
		s, err:=u.Snapshot(bootID); if err!=nil { return fmt.Errorf("snapshot %s: %w",service,err) }
		if err:=onSnapshot(s); err!=nil { return err }
	}

	signals:=make(chan *dbus.Signal,256)
	d.conn.Signal(signals); defer d.conn.RemoveSignal(signals)
	for {
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-d.conn.Done(): return fmt.Errorf("D-Bus connection lost")
		case sig:=<-signals:
			if sig==nil { return fmt.Errorf("D-Bus signal channel closed") }
			if sig.Name!=propertiesIF+".PropertiesChanged" || len(sig.Body)<2 { continue }
			u,ok:=monitor.byPath[sig.Path]; if !ok { continue }
			iface,ok:=sig.Body[0].(string); if !ok || iface!=unitIface { continue }
			props,ok:=sig.Body[1].(map[string]dbus.Variant); if !ok || !interesting(props) { continue }
			s,err:=u.Snapshot(bootID); if err!=nil { return fmt.Errorf("snapshot after PropertiesChanged: %w",err) }
			if err:=onSnapshot(s); err!=nil { return err }
		}
	}
}
