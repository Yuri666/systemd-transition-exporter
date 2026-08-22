package systemd

import (
	"context"
	"fmt"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

// RunResilient owns the complete D-Bus connection lifecycle. A failed
// connection is never translated into service downtime; the caller is
// explicitly notified through onConnection so it can expose collector
// connectivity as a separate Prometheus metric.
func RunResilient(
	ctx context.Context,
	services []string,
	reconnectInterval time.Duration,
	onSnapshot func(model.UnitSnapshot) error,
	onConnection func(connected bool, at time.Time),
) error {
	if reconnectInterval <= 0 {
		reconnectInterval = time.Second
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		d, err := Connect(ctx)
		if err != nil {
			onConnection(false, time.Now())
			if err := sleep(ctx, reconnectInterval); err != nil {
				return err
			}
			continue
		}

		monitor := NewMonitor(d)
		ok := true

		// Subscribe before loading snapshots. This closes the startup race in
		// which a transition can occur between discovery and AddMatch.
		if err := monitor.Subscribe(); err != nil {
			ok = false
			logConnectionError("D-Bus AddMatch", err)
		} else {
			for _, service := range services {
				u, err := d.LoadUnit(service)
				if err != nil {
					// One missing/broken unit must not tear down monitoring of all
					// other configured services.
					logConnectionError("LoadUnit "+service, err)
					continue
				}
				monitor.AddUnit(u)
			}

			if len(services) == 0 {
				ok = false
			}
		}

		if !ok {
			_ = d.Close()
			onConnection(false, time.Now())
			if err := sleep(ctx, reconnectInterval); err != nil {
				return err
			}
			continue
		}

		onConnection(true, time.Now())
		bootID, err := BootID()
		if err != nil {
			_ = d.Close()
			onConnection(false, time.Now())
			if err := sleep(ctx, reconnectInterval); err != nil {
				return err
			}
			continue
		}

		// Reconciliation snapshot after every successful connection. The
		// engine is responsible for deciding whether it represents a new
		// transition or merely the current state.
		for _, service := range services {
			if u := monitor.byService(service); u != nil {
				s, err := u.Snapshot(bootID)
				if err != nil {
					ok = false
					break
				}
				if err := onSnapshot(s); err != nil {
					_ = d.Close()
					return err
				}
			}
		}

		if !ok {
			_ = d.Close()
			onConnection(false, time.Now())
			if err := sleep(ctx, reconnectInterval); err != nil {
				return err
			}
			continue
		}

		err = monitor.Run(ctx, func(u *Unit) error {
			s, err := u.Snapshot(bootID)
			if err != nil {
				return err
			}
			return onSnapshot(s)
		})

		_ = d.Close()
		onConnection(false, time.Now())
		if err == context.Canceled || ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			logConnectionError("D-Bus monitor", err)
		}
		if err := sleep(ctx, reconnectInterval); err != nil {
			return err
		}
	}
}

func (m *Monitor) byService(service string) *Unit {
	for _, u := range m.byPath {
		if u != nil && u.Service() == service {
			return u
		}
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func logConnectionError(operation string, err error) {
	_ = fmt.Sprintf("%s: %v", operation, err)
}
