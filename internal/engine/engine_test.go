package engine

import (
	"testing"
	"time"

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

func TestOnlyActiveIsUp(t *testing.T) {
	states := []string{"inactive", "activating", "deactivating", "failed", "reloading", "maintenance", "unknown", ""}
	for _, activeState := range states {
		e := New()
		e.Apply(snapshot("test.service", activeState, 1000, 0))
		state, ok := e.State("test.service")
		if !ok {
			t.Fatalf("state missing for %q", activeState)
		}
		if state.Availability != model.StateDown {
			t.Fatalf("ActiveState=%q mapped to %v, want down", activeState, state.Availability)
		}
	}

	e := New()
	e.Apply(snapshot("test.service", "active", 1000, 0))
	state, _ := e.State("test.service")
	if state.Availability != model.StateUp {
		t.Fatalf("ActiveState=active mapped to %v, want up", state.Availability)
	}
}

func TestHostRebootMarksPreviouslyUpServiceDown(t *testing.T) {
	e := New()
	e.RestoreState(model.ServiceState{
		Service:      "pcscf.service",
		Availability: model.StateUp,
		BootID:       "old-boot",
	})
	at := time.Date(2026, 8, 24, 15, 2, 3, 0, time.UTC)
	events := e.ApplyReboot("new-boot", at)
	if len(events) != 1 {
		t.Fatalf("got %d reboot events, want 1", len(events))
	}
	if events[0].State != model.StateDown || events[0].Source != model.SourceHostReboot || events[0].EventTimeUnixMS != at.UnixMilli() {
		t.Fatalf("unexpected reboot event: %+v", events[0])
	}
	state, _ := e.State("pcscf.service")
	if state.Availability != model.StateDown || state.BootID != "new-boot" {
		t.Fatalf("unexpected post-reboot state: %+v", state)
	}
}

func TestHostRebootDoesNotEmitForAlreadyDownService(t *testing.T) {
	e := New()
	e.RestoreState(model.ServiceState{
		Service:      "pcscf.service",
		Availability: model.StateDown,
		BootID:       "old-boot",
	})
	if events := e.ApplyReboot("new-boot", time.Now()); len(events) != 0 {
		t.Fatalf("already-down service emitted reboot events: %+v", events)
	}
	state, _ := e.State("pcscf.service")
	if state.BootID != "new-boot" {
		t.Fatalf("boot id was not advanced: %+v", state)
	}
}
