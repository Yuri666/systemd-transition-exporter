package recovery

import (
	"testing"
	"time"
)

func TestRecoveryWindowUsesOnlyCurrentLocalSlot(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	ready := time.Date(2026, 8, 24, 15, 14, 0, 0, loc)
	start, end, ok := RecoveryWindow(ready, 15*time.Minute)
	if !ok {
		t.Fatal("expected a recovery window")
	}
	if !start.Equal(time.Date(2026, 8, 24, 15, 0, 0, 0, loc)) {
		t.Fatalf("window starts at %s, want 15:00", start)
	}
	if !end.Equal(ready) {
		t.Fatalf("window ends at %s, want %s", end, ready)
	}
}

func TestRecoveryWindowDoesNotIncludePreviousSlot(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	ready := time.Date(2026, 8, 24, 15, 14, 0, 0, loc)
	start, _, ok := RecoveryWindow(ready, 15*time.Minute)
	if !ok {
		t.Fatal("expected a recovery window")
	}
	previousSlot := time.Date(2026, 8, 24, 14, 45, 0, 0, loc)
	if !start.After(previousSlot) {
		t.Fatalf("window unexpectedly includes previous slot: start=%s", start)
	}
}

func TestRecoveryWindowAtSlotBoundaryIsEmpty(t *testing.T) {
	ready := time.Date(2026, 8, 24, 15, 0, 0, 0, time.Local)
	if _, _, ok := RecoveryWindow(ready, 15*time.Minute); ok {
		t.Fatal("exact slot boundary must produce an empty recovery interval")
	}
}
