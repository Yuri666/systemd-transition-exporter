package systemd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type MonitorV2 struct {
	mu sync.RWMutex
	conn *Connection
	services map[string]*Unit
	reconnectInterval time.Duration
}

func NewResilientMonitor(conn *Connection, reconnectInterval time.Duration) *MonitorV2 {
	if reconnectInterval <= 0 { reconnectInterval = time.Second }
	return &MonitorV2{conn: conn, services: make(map[string]*Unit), reconnectInterval: reconnectInterval}
}

func (m *MonitorV2) AddService(name string) { m.mu.Lock(); defer m.mu.Unlock(); m.services[name] = nil }

func (m *MonitorV2) Services() []string {
	m.mu.RLock(); defer m.mu.RUnlock()
	out := make([]string, 0, len(m.services)); for s := range m.services { out = append(out, s) }; return out
}

// Run reconnects the D-Bus connection after failures. After each successful
// connection it recreates unit objects and re-subscribes to PropertiesChanged.
// The caller performs reconciliation through the callback for every service.
func (m *MonitorV2) Run(ctx context.Context, onSnapshot func(model.UnitSnapshot) error) error {
	for {
		if err := m.connectAndSubscribe(ctx, onSnapshot); err != nil && ctx.Err() == nil {
			t := time.NewTimer(m.reconnectInterval)
			select { case <-ctx.Done(): t.Stop(); return ctx.Err(); case <-t.C: }
			continue
		}
		if ctx.Err() != nil { return ctx.Err() }
		return nil
	}
}

func (m *MonitorV2) connectAndSubscribe(ctx context.Context, onSnapshot func(model.UnitSnapshot) error) error {
	conn, err := Connect(ctx)
	if err != nil { return err }
	defer conn.Close()

	bootID, err := BootID(); if err != nil { return err }
	m.mu.Lock(); m.conn = conn; m.mu.Unlock()

	for _, service := range m.Services() {
		u, err := conn.LoadUnit(service)
		if err != nil { return fmt.Errorf("load %s: %w", service, err) }
		m.mu.Lock(); m.services[service] = u; m.mu.Unlock()
		if err := onSnapshotMust(conn, u, bootID, onSnapshot); err != nil { return err }
	}

	if err := conn.SubscribeForUnits(); err != nil { return err }

	for {
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-conn.Done(): return fmt.Errorf("D-Bus connection lost")
		case <-time.After(500 * time.Millisecond):
			// godbus signal delivery is handled by the connection's watcher;
			// reconciliation is deliberately periodic and lightweight.
			for _, service := range m.Services() {
				m.mu.RLock(); u := m.services[service]; m.mu.RUnlock()
				if u == nil { continue }
				if err := onSnapshotMust(conn, u, bootID, onSnapshot); err != nil { return err }
			}
		}
	}
}

func onSnapshotMust(conn *Connection, u *Unit, bootID string, cb func(model.UnitSnapshot) error) error {
	s, err := u.Snapshot(bootID); if err != nil { return err }; return cb(s)
}

// Keep dbus imported in this file while the signal/reconnect implementation is
// completed against the concrete connection wrapper.
var _ dbus.Signal
