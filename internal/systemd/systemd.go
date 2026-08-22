package systemd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

const (
	busName = "org.freedesktop.systemd1"
	managerPath = dbus.ObjectPath("/org/freedesktop/systemd1")
	managerIface = "org.freedesktop.systemd1.Manager"
	unitIface = "org.freedesktop.systemd1.Unit"
	propertiesIF = "org.freedesktop.DBus.Properties"
	dbusIF = "org.freedesktop.DBus"
	dbusPath = dbus.ObjectPath("/org/freedesktop/DBus")
)

type DBus struct { conn *dbus.Conn }

func Connect(context.Context) (*DBus, error) {
	conn, err := dbus.ConnectSystemBus()
	if err != nil { return nil, fmt.Errorf("connect system bus: %w", err) }
	return &DBus{conn: conn}, nil
}

func (d *DBus) Close() error {
	if d != nil && d.conn != nil { return d.conn.Close() }
	return nil
}

func (d *DBus) Conn() *dbus.Conn { return d.conn }

func BootID() (string, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil { return "", fmt.Errorf("read boot_id: %w", err) }
	return strings.TrimSpace(string(b)), nil
}

func BootTime() (time.Time, error) {
	b, err := os.ReadFile("/proc/uptime")
	if err != nil { return time.Time{}, fmt.Errorf("read uptime: %w", err) }
	f := strings.Fields(string(b))
	if len(f) == 0 { return time.Time{}, fmt.Errorf("invalid /proc/uptime") }
	sec, err := strconv.ParseFloat(f[0], 64)
	if err != nil { return time.Time{}, fmt.Errorf("parse uptime: %w", err) }
	return time.Now().Add(-time.Duration(sec * float64(time.Second))), nil
}

type Unit struct {
	conn *dbus.Conn
	path dbus.ObjectPath
	service string
}

func (u *Unit) Path() dbus.ObjectPath { return u.path }
func (u *Unit) Service() string { return u.service }
func (u *Unit) Object() dbus.BusObject { return u.conn.Object(busName, u.path) }

func (d *DBus) LoadUnit(service string) (*Unit, error) {
	c := d.conn.Object(busName, managerPath).Call(managerIface+".LoadUnit", 0, service)
	if c.Err != nil { return nil, fmt.Errorf("LoadUnit(%s): %w", service, c.Err) }
	var p dbus.ObjectPath
	if err := c.Store(&p); err != nil { return nil, fmt.Errorf("decode unit path: %w", err) }
	return &Unit{conn: d.conn, path: p, service: service}, nil
}

func (u *Unit) Snapshot(bootID string) (model.UnitSnapshot, error) {
	obj := u.Object()
	get := func(n string) (interface{}, error) {
		v, e := obj.GetProperty(unitIface + "." + n)
		if e != nil { return nil, fmt.Errorf("get %s: %w", n, e) }
		return v.Value(), nil
	}
	a, e := get("ActiveState"); if e != nil { return model.UnitSnapshot{}, e }
	s, e := get("SubState"); if e != nil { return model.UnitSnapshot{}, e }
	en, e := get("ActiveEnterTimestamp"); if e != nil { return model.UnitSnapshot{}, e }
	ex, e := get("ActiveExitTimestamp"); if e != nil { return model.UnitSnapshot{}, e }
	enm, e := get("ActiveEnterTimestampMonotonic"); if e != nil { return model.UnitSnapshot{}, e }
	exm, e := get("ActiveExitTimestampMonotonic"); if e != nil { return model.UnitSnapshot{}, e }
	as, ok := a.(string); if !ok { return model.UnitSnapshot{}, fmt.Errorf("ActiveState has type %T", a) }
	ss, ok := s.(string); if !ok { return model.UnitSnapshot{}, fmt.Errorf("SubState has type %T", s) }
	return model.UnitSnapshot{
		Service: u.service, ActiveState: as, SubState: ss,
		ActiveEnterTimestampUS: asUint64(en), ActiveExitTimestampUS: asUint64(ex),
		ActiveEnterTimestampMonotonicUS: asUint64(enm), ActiveExitTimestampMonotonicUS: asUint64(exm),
		BootID: bootID, ObservedAt: time.Now(),
	}, nil
}

func asUint64(v interface{}) uint64 {
	switch x := v.(type) {
	case uint64: return x
	case int64: if x >= 0 { return uint64(x) }
	case uint32: return uint64(x)
	}
	return 0
}

type Monitor struct { dbus *DBus; byPath map[dbus.ObjectPath]*Unit }

func NewMonitor(d *DBus) *Monitor { return &Monitor{dbus: d, byPath: map[dbus.ObjectPath]*Unit{}} }
func (m *Monitor) AddUnit(u *Unit) { m.byPath[u.Path()] = u }
func (m *Monitor) byService(s string) *Unit { for _, u := range m.byPath { if u != nil && u.Service() == s { return u } }; return nil }

func (m *Monitor) Subscribe() error {
	args := "type='signal',interface='org.freedesktop.DBus.Properties',member='PropertiesChanged',path_namespace='/org/freedesktop/systemd1/unit'"
	if e := m.dbus.conn.Object(dbusIF, dbusPath).Call(dbusIF+".AddMatch", 0, args).Err; e != nil { return fmt.Errorf("AddMatch: %w", e) }
	return nil
}

// Run monitors systemd unit property changes. Connection liveness is determined
// by the godbus connection context, not by an application-level Ping timeout.
// A slow or temporarily blocked systemd operation must not be interpreted as a
// transport disconnect: this is critical because service start/stop operations
// can legitimately make systemd busy for longer than a short health-check timeout.
func (m *Monitor) Run(ctx context.Context, handler func(*Unit) error) error {
	signals := make(chan *dbus.Signal, 256)
	m.dbus.conn.Signal(signals)
	defer m.dbus.conn.RemoveSignal(signals)

	connCtx := m.dbus.conn.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-connCtx.Done():
			return fmt.Errorf("D-Bus connection context closed: %w", connCtx.Err())
		case sig := <-signals:
			if sig == nil { return fmt.Errorf("D-Bus signal channel closed") }
			if sig.Name != propertiesIF+".PropertiesChanged" || len(sig.Body) < 2 { continue }
			u, ok := m.byPath[sig.Path]; if !ok { continue }
			iface, ok := sig.Body[0].(string); if !ok || iface != unitIface { continue }
			props, ok := sig.Body[1].(map[string]dbus.Variant); if !ok || !interesting(props) { continue }
			if e := handler(u); e != nil { return e }
		}
	}
}

func interesting(p map[string]dbus.Variant) bool {
	for n := range p {
		switch n {
		case "ActiveState", "SubState", "ActiveEnterTimestamp", "ActiveExitTimestamp", "ActiveEnterTimestampMonotonic", "ActiveExitTimestampMonotonic":
			return true
		}
	}
	return false
}
