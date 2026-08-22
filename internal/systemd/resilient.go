package systemd

import (
	"context"
	"fmt"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

// RunResilient owns the D-Bus connection lifecycle.
//
// CONNECTED is reported only after the system bus connection, configured unit
// lookup, signal subscription, and initial snapshots all succeed. A Ping is
// deliberately not used as a disconnect detector: a busy systemd can delay a
// perfectly healthy D-Bus request. Once monitoring starts, Monitor.Run observes
// the godbus connection context and the signal stream; only an actual connection
// termination or monitor error moves the lifecycle to DISCONNECTED.
func RunResilient(
	ctx context.Context,
	services []string,
	reconnectInterval time.Duration,
	onSnapshot func(model.UnitSnapshot) error,
	onConnectionState func(bool, time.Time),
	onConnectionError func(error),
) error {
	if reconnectInterval <= 0 {
		reconnectInterval = time.Second
	}

	connectedOnce := false

	for {
		if connectedOnce {
			timer := time.NewTimer(reconnectInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}

		d, err := Connect(ctx)
		if err != nil {
			if ctx.Err() != nil { return ctx.Err() }
			if onConnectionError != nil { onConnectionError(fmt.Errorf("connect system D-Bus: %w", err)) }
			continue
		}

		m := NewMonitor(d)
		setupFailed := false

		for _, service := range services {
			u, e := d.LoadUnit(service)
			if e != nil {
				setupFailed = true
				if onConnectionError != nil { onConnectionError(fmt.Errorf("load systemd unit %s: %w", service, e)) }
				break
			}
			m.AddUnit(u)
		}

		if !setupFailed {
			if err := m.Subscribe(); err != nil {
				setupFailed = true
				if onConnectionError != nil { onConnectionError(fmt.Errorf("subscribe to systemd signals: %w", err)) }
			}
		}

		if setupFailed {
			_ = d.Close()
			continue
		}

		bootID, err := BootID()
		if err != nil {
			_ = d.Close()
			return fmt.Errorf("read boot id: %w", err)
		}

		for _, service := range services {
			u := m.byService(service)
			if u == nil { continue }
			s, e := u.Snapshot(bootID)
			if e != nil {
				setupFailed = true
				if onConnectionError != nil { onConnectionError(fmt.Errorf("initial snapshot %s: %w", service, e)) }
				break
			}
			if e = onSnapshot(s); e != nil {
				_ = d.Close()
				return e
			}
		}

		if setupFailed {
			_ = d.Close()
			continue
		}

		if onConnectionState != nil { onConnectionState(true, time.Now()) }
		connectedOnce = true

		runErr := m.Run(ctx, func(u *Unit) error {
			s, e := u.Snapshot(bootID)
			if e != nil { return e }
			return onSnapshot(s)
		})

		_ = d.Close()
		if ctx.Err() != nil { return ctx.Err() }

		if onConnectionState != nil { onConnectionState(false, time.Now()) }
		if onConnectionError != nil && runErr != nil {
			onConnectionError(fmt.Errorf("systemd D-Bus monitoring lost: %w", runErr))
		}
	}
}
