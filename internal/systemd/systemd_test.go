package systemd

import (
	"strings"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"
)

func unitProperties() map[string]dbus.Variant {
	return map[string]dbus.Variant{
		"ActiveState":                   dbus.MakeVariant("active"),
		"SubState":                      dbus.MakeVariant("running"),
		"ActiveEnterTimestamp":          dbus.MakeVariant(uint64(1789646355520000)),
		"ActiveExitTimestamp":           dbus.MakeVariant(uint64(1789646281948000)),
		"ActiveEnterTimestampMonotonic": dbus.MakeVariant(uint64(920000000)),
		"ActiveExitTimestampMonotonic":  dbus.MakeVariant(uint64(846000000)),
	}
}

// The observation time is passed in by the caller because it is taken before
// the properties are read: a timestamp later than the state it describes would
// place a stale availability sample after the transition that ended it.
func TestSnapshotFromPropertiesKeepsCallerObservationTime(t *testing.T) {
	observedAt := time.Date(2026, 9, 17, 14, 59, 15, int(300*time.Millisecond), time.Local)
	snapshot, err := snapshotFromProperties("icscf.service", "boot", observedAt, unitProperties())
	if err != nil {
		t.Fatalf("snapshotFromProperties: %v", err)
	}
	if !snapshot.ObservedAt.Equal(observedAt) {
		t.Fatalf("ObservedAt = %s, want %s", snapshot.ObservedAt, observedAt)
	}
	if snapshot.Service != "icscf.service" || snapshot.BootID != "boot" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if snapshot.ActiveState != "active" || snapshot.SubState != "running" {
		t.Fatalf("state = %q/%q", snapshot.ActiveState, snapshot.SubState)
	}
	if snapshot.ActiveEnterTimestampUS != 1789646355520000 || snapshot.ActiveExitTimestampUS != 1789646281948000 {
		t.Fatalf("timestamps = %d/%d", snapshot.ActiveEnterTimestampUS, snapshot.ActiveExitTimestampUS)
	}
	if snapshot.ActiveEnterTimestampMonotonicUS != 920000000 || snapshot.ActiveExitTimestampMonotonicUS != 846000000 {
		t.Fatalf("monotonic timestamps = %d/%d", snapshot.ActiveEnterTimestampMonotonicUS, snapshot.ActiveExitTimestampMonotonicUS)
	}
}

// A partial answer must fail rather than produce a snapshot with a zero
// timestamp, which the engine would later mistake for a fresh transition.
func TestSnapshotFromPropertiesRejectsIncompleteAnswer(t *testing.T) {
	for _, name := range []string{"ActiveState", "SubState", "ActiveEnterTimestamp", "ActiveExitTimestamp", "ActiveEnterTimestampMonotonic", "ActiveExitTimestampMonotonic"} {
		props := unitProperties()
		delete(props, name)
		_, err := snapshotFromProperties("icscf.service", "boot", time.Now(), props)
		if err == nil {
			t.Fatalf("missing %s was accepted", name)
		}
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("error for missing %s = %v", name, err)
		}
	}
}
