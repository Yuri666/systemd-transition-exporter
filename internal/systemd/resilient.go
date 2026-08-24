package systemd

import (
	"context"
	"fmt"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

// RunResilient owns the D-Bus connection lifecycle. A method timeout is not a
// transport disconnect. Disconnect is reported only after Monitor.Run observes
// the godbus connection context ending or the monitor itself fails.
func RunResilient(ctx context.Context, services []string, reconnectInterval time.Duration,
	onSnapshot func(model.UnitSnapshot) error,
	onConnectionState func(bool, time.Time),
	onConnectionError func(error),
	onRecovery func(time.Time, time.Time) error,
) error {
	if reconnectInterval <= 0 { reconnectInterval = time.Second }
	connectedOnce := false
	var disconnectedAt time.Time
	for {
		if connectedOnce {
			timer := time.NewTimer(reconnectInterval)
			select { case <-ctx.Done(): timer.Stop(); return ctx.Err(); case <-timer.C: }
		}
		d, err := Connect(ctx)
		if err != nil {
			if ctx.Err() != nil { return ctx.Err() }
			if onConnectionError != nil { onConnectionError(fmt.Errorf("connect system D-Bus: %w", err)) }
			continue
		}
		SetDebugDBus(d)
		m := NewMonitor(d)
		setupFailed := false
		for _, service := range services {
			u, e := d.LoadUnit(service)
			if e != nil { setupFailed = true; if onConnectionError != nil { onConnectionError(fmt.Errorf("load systemd unit %s: %w", service, e)) }; break }
			m.AddUnit(u)
		}
		if !setupFailed {
			if err := m.Subscribe(); err != nil { setupFailed = true; if onConnectionError != nil { onConnectionError(fmt.Errorf("subscribe to systemd signals: %w", err)) } }
		}
		if setupFailed { _ = d.Close(); SetDebugDBus(nil); continue }
		bootID, err := BootID()
		if err != nil { _ = d.Close(); SetDebugDBus(nil); return fmt.Errorf("read boot id: %w", err) }

		// Recover the historical gap before taking the reconnect snapshot.
		// This guarantees that Remote Write receives historical samples before
		// the current-state heartbeat queued by onSnapshot, preserving the
		// per-series timestamp ordering required by Remote Write.
		if !disconnectedAt.IsZero() && onRecovery != nil {
			until := time.Now()
			if err := onRecovery(disconnectedAt, until); err != nil && onConnectionError != nil { onConnectionError(fmt.Errorf("journal recovery: %w", err)) }
			disconnectedAt = time.Time{}
		}

		for _, service := range services {
			u := m.byService(service); if u == nil { continue }
			s, e := u.Snapshot(bootID)
			if e != nil { setupFailed = true; if onConnectionError != nil { onConnectionError(fmt.Errorf("initial snapshot %s: %w", service, e)) }; break }
			if e = onSnapshot(s); e != nil { _ = d.Close(); SetDebugDBus(nil); return e }
		}
		if setupFailed { _ = d.Close(); SetDebugDBus(nil); continue }
		if onConnectionState != nil { onConnectionState(true, time.Now()) }
		connectedOnce = true
		runErr := m.Run(ctx, func(u *Unit) error {
			s, e := u.Snapshot(bootID); if e != nil { return e }
			return onSnapshot(s)
		})
		SetDebugDBus(nil)
		_ = d.Close()
		if ctx.Err() != nil { return ctx.Err() }
		disconnectedAt = time.Now()
		if onConnectionState != nil { onConnectionState(false, disconnectedAt) }
		if onConnectionError != nil && runErr != nil { onConnectionError(fmt.Errorf("systemd D-Bus monitoring lost: %w", runErr)) }
	}
}
