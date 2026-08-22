package engine

import (
	"testing"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

func snapshot(service, state string, enter, exit uint64) model.UnitSnapshot {
	return model.UnitSnapshot{
		Service: service, ActiveState: state, SubState: state,
		ActiveEnterTimestampUS: enter, ActiveExitTimestampUS: exit,
		ActiveEnterTimestampMonotonicUS: enter, ActiveExitTimestampMonotonicUS: exit,
		BootID: "boot-1",
	}
}

func TestInitialSnapshotDoesNotEmit(t *testing.T) {
	e := New()
	if got := e.Apply(snapshot("pcscf.service", "active", 1000, 0)); len(got) != 0 {
		t.Fatalf("initial snapshot emitted %d events", len(got))
	}
}

func TestDownTransition(t *testing.T) {
	e := New()
	e.Apply(snapshot("pcscf.service", "active", 1000, 0))
	got := e.Apply(snapshot("pcscf.service", "inactive", 1000, 2000))
	if len(got) != 1 || got[0].State != model.StateDown || got[0].EventTimeUnixMS != 2 {
		t.Fatalf("unexpected events: %+v", got)
	}
}

func TestMultipleTransitionsBetweenSnapshots(t *testing.T) {
	e := New()
	e.Apply(snapshot("pcscf.service", "active", 1000, 0))
	got := e.Apply(snapshot("pcscf.service", "active", 3000, 2000))
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d: %+v", len(got), got)
	}
	if got[0].State != model.StateDown || got[0].EventTimeUnixMS != 2 {
		t.Fatalf("first event: %+v", got[0])
	}
	if got[1].State != model.StateUp || got[1].EventTimeUnixMS != 3 {
		t.Fatalf("second event: %+v", got[1])
	}
}

func TestTransitionsAreOrderedByTimestamp(t *testing.T) {
	e := New()
	e.Apply(snapshot("pcscf.service", "active", 1000, 0))
	got := e.Apply(snapshot("pcscf.service", "active", 3000, 2000))
	if got[0].EventTimeUnixMS >= got[1].EventTimeUnixMS {
		t.Fatalf("events not ordered: %+v", got)
	}
}

func TestRestartWithSameBootDoesNotInventTransition(t *testing.T) {
	e := New()
	s := snapshot("pcscf.service", "active", 1000, 0)
	e.Apply(s)
	if got := e.Apply(s); len(got) != 0 {
		t.Fatalf("unchanged snapshot emitted %+v", got)
	}
}
