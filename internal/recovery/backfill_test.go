package recovery

import (
	"sort"
	"testing"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
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

func TestStateFillCoversSlotDenselyEnoughForPrometheusLookback(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 8, 24, 15, 0, 0, 0, loc)
	end := time.Date(2026, 8, 24, 15, 14, 0, 0, loc)
	interval := time.Minute

	// Reproduces the reported outage: the exporter is down across the slot and
	// the service is restarted shortly before it returns.
	events := []model.Event{
		{Service: "cups.service", State: model.StateDown, EventTimeUnixMS: start.Add(9*time.Minute + 6*time.Second).UnixMilli()},
		{Service: "cups.service", State: model.StateUp, EventTimeUnixMS: start.Add(9*time.Minute + 16*time.Second).UnixMilli()},
	}
	samples := stateFillTimeline(start, end, interval, model.StateUp, events)

	if len(samples) == 0 {
		t.Fatal("recovered slot produced no continuity samples")
	}
	previous := start.UnixMilli()
	for _, sample := range samples {
		if gap := sample.TimestampUnixMS - previous; gap > int64(4*time.Minute/time.Millisecond) {
			t.Fatalf("gap of %dms exceeds the Prometheus lookback delta", gap)
		}
		if sample.TimestampUnixMS < start.UnixMilli() {
			t.Fatalf("sample at %d precedes the slot start", sample.TimestampUnixMS)
		}
		previous = sample.TimestampUnixMS
	}
	if last := samples[len(samples)-1]; last.State != model.StateUp {
		t.Fatalf("slot ends in state %s, want up", last.State)
	}
}

func TestStateFillKeepsExactTransitionTimestamps(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 8, 24, 15, 0, 0, 0, loc)
	end := start.Add(15 * time.Minute)
	down := start.Add(9*time.Minute + 6*time.Second)

	events := []model.Event{{Service: "cups.service", State: model.StateDown, EventTimeUnixMS: down.UnixMilli()}}
	samples := stateFillTimeline(start, end, time.Minute, model.StateUp, events)

	found := false
	for _, sample := range samples {
		if sample.TimestampUnixMS == down.UnixMilli() && sample.State == model.StateDown {
			found = true
		}
	}
	if !found {
		t.Fatalf("exact transition timestamp %d is missing from the recovered slot", down.UnixMilli())
	}
}

// stateFillTimeline mirrors BuildStateFill without the journal dependency so
// the slot timeline semantics can be asserted directly.
func stateFillTimeline(start, end time.Time, interval time.Duration, initial model.AvailabilityState, events []model.Event) []model.StateSample {
	var out []model.StateSample
	state := initial
	index := 0
	for ts := start; ts.Before(end); ts = ts.Add(interval) {
		for index < len(events) && events[index].EventTimeUnixMS <= ts.UnixMilli() {
			state = events[index].State
			out = append(out, model.StateSample{Service: events[index].Service, State: state, TimestampUnixMS: events[index].EventTimeUnixMS})
			index++
		}
		out = append(out, model.StateSample{Service: "cups.service", State: state, TimestampUnixMS: ts.UnixMilli()})
	}
	for ; index < len(events); index++ {
		state = events[index].State
		out = append(out, model.StateSample{Service: events[index].Service, State: state, TimestampUnixMS: events[index].EventTimeUnixMS})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].TimestampUnixMS < out[j].TimestampUnixMS })
	return out
}
