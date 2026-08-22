package systemd

import (
    "context"
    "time"

    "github.com/Yuri666/systemd-transition-exporter/internal/model"
)

// RunResilient owns the D-Bus connection lifecycle. A failed Peer.Ping is
// treated as transport loss; a fresh connection is established, subscriptions
// are recreated, and all configured units are reconciled before live signals.
func RunResilient(ctx context.Context, services []string, reconnectInterval time.Duration, onSnapshot func(model.UnitSnapshot) error, onConnectionState func(bool, time.Time)) error {
    if reconnectInterval <= 0 { reconnectInterval = time.Second }
    first := true
    for {
        if !first {
            timer := time.NewTimer(reconnectInterval)
            select { case <-ctx.Done(): timer.Stop(); return ctx.Err(); case <-timer.C: }
        }
        first = false

        d, err := Connect(ctx)
        if err != nil {
            if ctx.Err() != nil { return ctx.Err() }
            if onConnectionState != nil { onConnectionState(false, time.Now()) }
            continue
        }

        m := NewMonitor(d)
        failed := false
        for _, service := range services {
            u, e := d.LoadUnit(service)
            if e != nil { failed = true; break }
            m.AddUnit(u)
        }
        if failed || m.Subscribe() != nil {
            _ = d.Close()
            if onConnectionState != nil { onConnectionState(false, time.Now()) }
            continue
        }
        if err := m.Ping(ctx); err != nil {
            _ = d.Close()
            if onConnectionState != nil { onConnectionState(false, time.Now()) }
            continue
        }

        bootID, err := BootID()
        if err != nil { _ = d.Close(); return err }
        for _, service := range services {
            u := m.byService(service)
            if u == nil { continue }
            s, e := u.Snapshot(bootID)
            if e != nil { failed = true; break }
            if e = onSnapshot(s); e != nil { _ = d.Close(); return e }
        }
        if failed {
            _ = d.Close()
            if onConnectionState != nil { onConnectionState(false, time.Now()) }
            continue
        }

        if onConnectionState != nil { onConnectionState(true, time.Now()) }
        _ = m.Run(ctx, onSnapshot)
        _ = d.Close()
        if ctx.Err() != nil { return ctx.Err() }
        if onConnectionState != nil { onConnectionState(false, time.Now()) }
    }
}
