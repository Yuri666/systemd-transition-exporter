package wal

import (
	"testing"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

func TestStateSurvivesReopenWithoutTransitionEvents(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	want := model.ServiceState{
		Service:      "pcscf.service",
		Availability: model.StateUp,
		ActiveState:  "active",
		BootID:       "boot-1",
	}
	if err := w.SaveState(want); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	states := reopened.States()
	if len(states) != 1 {
		t.Fatalf("got %d persisted states, want 1", len(states))
	}
	if states[0].Service != want.Service || states[0].Availability != want.Availability || states[0].BootID != want.BootID {
		t.Fatalf("unexpected persisted state: %+v", states[0])
	}
}
