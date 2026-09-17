package systemd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
	"github.com/godbus/dbus/v5"
)

const (
	busName      = "org.freedesktop.systemd1"
	managerPath  = dbus.ObjectPath("/org/freedesktop/systemd1")
	managerIface = "org.freedesktop.systemd1.Manager"
	unitIface    = "org.freedesktop.systemd1.Unit"
	propertiesIF = "org.freedesktop.DBus.Properties"
	dbusIF       = "org.freedesktop.DBus"
	dbusPath     = dbus.ObjectPath("/org/freedesktop/DBus")
)

type DBus struct{ conn *dbus.Conn }

func Connect(context.Context) (*DBus, error) {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return nil, fmt.Errorf("connect system bus: %w", err)
	}
	return &DBus{conn: conn}, nil
}

func (d *DBus) Close() error {
	if d != nil && d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

func (d *DBus) Conn() *dbus.Conn { return d.conn }

func BootID() (string, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read boot_id: %w", err)
	}
	return model.CanonicalBootID(string(b)), nil
}

func BootTime() (time.Time, error) {
	b, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return time.Time{}, fmt.Errorf("read uptime: %w", err)
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return time.Time{}, fmt.Errorf("invalid /proc/uptime")
	}
	sec, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse uptime: %w", err)
	}
	return time.Now().Add(-time.Duration(sec * float64(time.Second))), nil
}

// CurrentStates reports the present availability of the configured units. The
// startup recovery slot is built before the monitor runs, so without this a
// freshly installed exporter with an empty WAL would have no authoritative
// state for the beginning of the slot.
func CurrentStates(ctx context.Context, services []string) (map[string]model.AvailabilityState, error) {
	d, err := Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	states := make(map[string]model.AvailabilityState, len(services))
	for _, service := range services {
		unit, err := d.LoadUnit(service)
		if err != nil {
			return nil, err
		}
		snapshot, err := unit.Snapshot("")
		if err != nil {
			return nil, err
		}
		states[service] = model.AvailabilityFromActiveState(snapshot.ActiveState)
	}
	return states, nil
}

type Unit struct {
	conn    *dbus.Conn
	path    dbus.ObjectPath
	service string
}

func (u *Unit) Path() dbus.ObjectPath  { return u.path }
func (u *Unit) Service() string        { return u.service }
func (u *Unit) Object() dbus.BusObject { return u.conn.Object(busName, u.path) }

func (d *DBus) LoadUnit(service string) (*Unit, error) {
	c := d.conn.Object(busName, managerPath).Call(managerIface+".LoadUnit", 0, service)
	if c.Err != nil {
		return nil, fmt.Errorf("LoadUnit(%s): %w", service, c.Err)
	}
	var p dbus.ObjectPath
	if err := c.Store(&p); err != nil {
		return nil, fmt.Errorf("decode unit path: %w", err)
	}
	return &Unit{conn: d.conn, path: p, service: service}, nil
}

// Snapshot reads the unit properties in a single D-Bus call. Reading them one
// at a time spread the answer over six round trips, and systemd was free to
// activate the unit in between: the result then mixed an ActiveState from
// before the activation with an ActiveEnterTimestamp from after it, so the
// snapshot reported a unit as down while already carrying the timestamp of its
// start. The observation time is taken before the call, so it can never be
// later than the state it describes.
func (u *Unit) Snapshot(bootID string) (model.UnitSnapshot, error) {
	observedAt := time.Now()
	call := u.Object().Call(propertiesIF+".GetAll", 0, unitIface)
	if call.Err != nil {
		return model.UnitSnapshot{}, fmt.Errorf("GetAll(%s): %w", u.service, call.Err)
	}
	var props map[string]dbus.Variant
	if err := call.Store(&props); err != nil {
		return model.UnitSnapshot{}, fmt.Errorf("decode properties of %s: %w", u.service, err)
	}
	return snapshotFromProperties(u.service, bootID, observedAt, props)
}

func snapshotFromProperties(service, bootID string, observedAt time.Time, props map[string]dbus.Variant) (model.UnitSnapshot, error) {
	text := func(name string) (string, error) {
		v, ok := props[name]
		if !ok {
			return "", fmt.Errorf("property %s of %s is missing", name, service)
		}
		s, ok := v.Value().(string)
		if !ok {
			return "", fmt.Errorf("%s has type %T", name, v.Value())
		}
		return s, nil
	}
	timestamp := func(name string) (uint64, error) {
		v, ok := props[name]
		if !ok {
			return 0, fmt.Errorf("property %s of %s is missing", name, service)
		}
		return asUint64(v.Value()), nil
	}
	activeState, err := text("ActiveState")
	if err != nil {
		return model.UnitSnapshot{}, err
	}
	subState, err := text("SubState")
	if err != nil {
		return model.UnitSnapshot{}, err
	}
	enter, err := timestamp("ActiveEnterTimestamp")
	if err != nil {
		return model.UnitSnapshot{}, err
	}
	exit, err := timestamp("ActiveExitTimestamp")
	if err != nil {
		return model.UnitSnapshot{}, err
	}
	enterMono, err := timestamp("ActiveEnterTimestampMonotonic")
	if err != nil {
		return model.UnitSnapshot{}, err
	}
	exitMono, err := timestamp("ActiveExitTimestampMonotonic")
	if err != nil {
		return model.UnitSnapshot{}, err
	}
	return model.UnitSnapshot{
		Service: service, ActiveState: activeState, SubState: subState,
		ActiveEnterTimestampUS: enter, ActiveExitTimestampUS: exit,
		ActiveEnterTimestampMonotonicUS: enterMono, ActiveExitTimestampMonotonicUS: exitMono,
		BootID: bootID, ObservedAt: observedAt,
	}, nil
}

func asUint64(v interface{}) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case int64:
		if x >= 0 {
			return uint64(x)
		}
	case uint32:
		return uint64(x)
	}
	return 0
}

type Monitor struct {
	dbus   *DBus
	byPath map[dbus.ObjectPath]*Unit
}

func NewMonitor(d *DBus) *Monitor  { return &Monitor{dbus: d, byPath: map[dbus.ObjectPath]*Unit{}} }
func (m *Monitor) AddUnit(u *Unit) { m.byPath[u.Path()] = u }
func (m *Monitor) byService(s string) *Unit {
	for _, u := range m.byPath {
		if u != nil && u.Service() == s {
			return u
		}
	}
	return nil
}

func (m *Monitor) Subscribe() error {
	args := "type='signal',interface='org.freedesktop.DBus.Properties',member='PropertiesChanged',path_namespace='/org/freedesktop/systemd1/unit'"
	if e := m.dbus.conn.Object(dbusIF, dbusPath).Call(dbusIF+".AddMatch", 0, args).Err; e != nil {
		return fmt.Errorf("AddMatch: %w", e)
	}
	return nil
}

// Run monitors systemd unit property changes. Connection liveness is determined
// by the godbus connection context, not by an application-level Ping timeout.
// A slow or temporarily blocked systemd operation must not be interpreted as a
// transport disconnect: this is critical because service start/stop operations
// can legitimately make systemd busy for longer than a short health-check timeout.
func (m *Monitor) Run(ctx context.Context, reconciliationInterval time.Duration, handler func(*Unit) error) error {
	signals := make(chan *dbus.Signal, 256)
	m.dbus.conn.Signal(signals)
	defer m.dbus.conn.RemoveSignal(signals)

	if reconciliationInterval <= 0 {
		reconciliationInterval = 30 * time.Second
	}
	reconcile := time.NewTicker(reconciliationInterval)
	defer reconcile.Stop()
	connCtx := m.dbus.conn.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-connCtx.Done():
			return fmt.Errorf("D-Bus connection context closed: %w", connCtx.Err())
		case <-reconcile.C:
			for _, u := range m.byPath {
				if e := handler(u); e != nil {
					return e
				}
			}
		case sig := <-signals:
			if sig == nil {
				return fmt.Errorf("D-Bus signal channel closed")
			}
			if sig.Name != propertiesIF+".PropertiesChanged" || len(sig.Body) < 2 {
				continue
			}
			u, ok := m.byPath[sig.Path]
			if !ok {
				continue
			}
			iface, ok := sig.Body[0].(string)
			if !ok || iface != unitIface {
				continue
			}
			props, ok := sig.Body[1].(map[string]dbus.Variant)
			if !ok || !interesting(props) {
				continue
			}
			if e := handler(u); e != nil {
				return e
			}
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
