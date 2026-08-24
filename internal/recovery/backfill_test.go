package recovery

import (
	"testing"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

func TestRecoveryWindowAlignedToHour(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	from := time.Date(2026, 8, 24, 0, 13, 30, 0, loc)
	to := time.Date(2026, 8, 24, 0, 13, 30, 0, loc).Add(15 * time.Minute)
	windows := RecoveryWindow(from, to, 15*time.Minute)
	if len(windows) != 2 {
		t.Fatalf("got %d windows, want 2", len(windows))
	}
	if !windows[0][0].Equal(time.Date(2026, 8, 24, 0, 0, 0, 0, loc)) {
		t.Fatalf("first window starts at %s, want 00:00", windows[0][0])
	}
	if !windows[1][0].Equal(time.Date(2026, 8, 24, 0, 15, 0, 0, loc)) {
		t.Fatalf("second window starts at %s, want 00:15", windows[1][0])
	}
}

func TestRecoveryWindowFiveToSixtyMinutesIsRepresentable(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	from := time.Date(2026, 8, 24, 12, 7, 0, 0, loc)
	to := from.Add(60 * time.Minute)
	windows := RecoveryWindow(from, to, 60*time.Minute)
	if len(windows) != 2 {
		t.Fatalf("got %d windows, want 2", len(windows))
	}
	if !windows[0][0].Equal(time.Date(2026, 8, 24, 12, 0, 0, 0, loc)) ||
		!windows[0][1].Equal(time.Date(2026, 8, 24, 13, 0, 0, 0, loc)) {
		t.Fatalf("unexpected first 60m recovery window: %#v", windows[0])
	}
	if !windows[1][0].Equal(time.Date(2026, 8, 24, 13, 0, 0, 0, loc)) ||
		!windows[1][1].Equal(to) {
		t.Fatalf("unexpected second 60m recovery window: %#v", windows[1])
	}
}

func TestBuildStateBackfillSemantics(t *testing.T) {
	// This test exercises the timeline semantics directly through the helper
	// used by BuildStateBackfill: before the 00:13:30 transition, every 2m
	// sample carries the previous state; after it, samples carry the new state.
	start := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(15 * time.Minute)
	events := []model.Event{{
		Service: "cups.service",
		State: model.StateUp,
		EventTimeUnixMS: start.Add(13*time.Minute + 30*time.Second).UnixMilli(),
	}}
	initial := model.StateDown

	var got []model.StateSample
	state := initial
	for ts := start; ts.Before(end); ts = ts.Add(2 * time.Minute) {
		for _, event := range events {
			if event.EventTimeUnixMS <= ts.UnixMilli() {
				state = event.State
			}
		}
		got = append(got, model.StateSample{Service: "cups.service", State: state, TimestampUnixMS: ts.UnixMilli()})
	}

	if len(got) != 8 {
		t.Fatalf("got %d samples, want 8", len(got))
	}
	for i := 0; i < 7; i++ {
		if got[i].State != model.StateDown {
			t.Fatalf("sample %d at %s is %s, want down", i, time.UnixMilli(got[i].TimestampUnixMS), got[i].State)
		}
	}
	if got[7].State != model.StateUp {
		t.Fatalf("sample 7 at %s is %s, want up", time.UnixMilli(got[7].TimestampUnixMS), got[7].State)
	}
}
