package recovery

import (
	"context"
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

func TestSlotOpeningTimeIsFractionAfterSlotStart(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 9, 4, 15, 0, 0, 0, loc)
	got := SlotOpeningTime(start)
	want := time.Date(2026, 9, 4, 15, 0, 0, int(5*time.Millisecond), loc)
	if !got.Equal(want) {
		t.Fatalf("SlotOpeningTime = %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestDueSlotOpeningOnlyInsideOpenSlotAfterFraction(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 9, 4, 15, 0, 0, 0, loc)
	opening := SlotOpeningTime(start)
	if due := DueSlotOpening(start, 15*time.Minute); !due.IsZero() {
		t.Fatalf("opening is not due on the slot boundary, got %s", due)
	}
	if due := DueSlotOpening(opening, 15*time.Minute); !due.Equal(opening) {
		t.Fatalf("opening is due at the fraction instant, got %s", due)
	}
	if due := DueSlotOpening(start.Add(15*time.Minute), 15*time.Minute); !due.IsZero() {
		t.Fatalf("next slot must not report the previous opening, got %s", due)
	}
}

func TestNextSlotOpeningSkipsAnInstantThatHasPassed(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 9, 4, 15, 0, 0, 0, loc)
	opening := SlotOpeningTime(start)
	if got := NextSlotOpening(start, 15*time.Minute); !got.Equal(opening) {
		t.Fatalf("next opening = %s, want current %s", got, opening)
	}
	next := SlotOpeningTime(start.Add(15 * time.Minute))
	if got := NextSlotOpening(opening, 15*time.Minute); !got.Equal(next) {
		t.Fatalf("next opening after due instant = %s, want %s", got, next)
	}
}

func TestSlotClosingTimeIsLeadBeforeSlotEnd(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 9, 4, 15, 0, 0, 0, loc)
	got := SlotClosingTime(start, 15*time.Minute)
	want := time.Date(2026, 9, 4, 15, 14, 59, int(5*time.Millisecond), loc)
	if !got.Equal(want) {
		t.Fatalf("SlotClosingTime = %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestDueSlotClosingOnlyInsideOpenSlotAfterLead(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 9, 4, 15, 0, 0, 0, loc)
	closing := SlotClosingTime(start, 15*time.Minute)
	if due := DueSlotClosing(closing.Add(-time.Millisecond), 15*time.Minute); !due.IsZero() {
		t.Fatalf("closing is not due before the lead instant, got %s", due)
	}
	if due := DueSlotClosing(closing, 15*time.Minute); !due.Equal(closing) {
		t.Fatalf("closing is due at the lead instant, got %s", due)
	}
	if due := DueSlotClosing(start.Add(15*time.Minute), 15*time.Minute); !due.IsZero() {
		t.Fatalf("next slot must not report the previous closing, got %s", due)
	}
}

func TestNextSlotClosingSkipsAnInstantThatHasPassed(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 9, 4, 15, 0, 0, 0, loc)
	closing := SlotClosingTime(start, 15*time.Minute)
	if got := NextSlotClosing(closing.Add(-time.Second), 15*time.Minute); !got.Equal(closing) {
		t.Fatalf("next closing = %s, want current %s", got, closing)
	}
	next := SlotClosingTime(start.Add(15*time.Minute), 15*time.Minute)
	if got := NextSlotClosing(closing, 15*time.Minute); !got.Equal(next) {
		t.Fatalf("next closing after due instant = %s, want %s", got, next)
	}
}

func TestStateFillDoesNotPublishSlotClosingSample(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 9, 4, 15, 0, 0, 0, loc)
	end := start.Add(15 * time.Minute)
	current := map[string]model.AvailabilityState{"cups.service": model.StateUp}
	samples, err := BuildStateFill(context.Background(), []string{"cups.service"}, start, end, nil, time.Minute, current)
	if err != nil {
		t.Fatalf("BuildStateFill: %v", err)
	}
	opening := SlotOpeningTime(start).UnixMilli()
	closing := SlotClosingTime(start, 15*time.Minute).UnixMilli()
	for _, sample := range samples {
		if sample.TimestampUnixMS == opening || sample.TimestampUnixMS == closing {
			t.Fatal("recovery fill must not invent the live slot-edge samples")
		}
	}
}

func TestSlotStartStateUsesCurrentStateWhenSlotHasNoTransition(t *testing.T) {
	start := time.Date(2026, 8, 27, 14, 30, 0, 0, time.Local)
	current := map[string]model.AvailabilityState{"cups.service": model.StateUp}

	state, err := slotStartState(context.Background(), "cups.service", start, nil, current)
	if err != nil {
		t.Fatalf("slotStartState: %v", err)
	}
	if state != model.StateUp {
		t.Fatalf("slot start state = %s, want up", state)
	}
}

func TestSlotStartStateIsTheInverseOfTheFirstTransition(t *testing.T) {
	start := time.Date(2026, 8, 27, 14, 30, 0, 0, time.Local)
	for _, tc := range []struct {
		first model.AvailabilityState
		want  model.AvailabilityState
	}{
		{first: model.StateUp, want: model.StateDown},
		{first: model.StateDown, want: model.StateUp},
	} {
		events := []model.Event{{Service: "cups.service", State: tc.first, EventTimeUnixMS: start.Add(time.Minute).UnixMilli()}}
		state, err := slotStartState(context.Background(), "cups.service", start, events, nil)
		if err != nil {
			t.Fatalf("slotStartState: %v", err)
		}
		if state != tc.want {
			t.Fatalf("first transition %s produced slot start state %s, want %s", tc.first, state, tc.want)
		}
	}
}

func TestStateFillKeepsLongRunningServiceUpOnFirstStart(t *testing.T) {
	loc := time.FixedZone("TEST", 3*60*60)
	start := time.Date(2026, 8, 27, 14, 30, 0, 0, loc)
	end := start.Add(8 * time.Minute)

	// A first start with an empty WAL: the unit has been active since long
	// before the slot, so the journal holds no lifecycle record inside it.
	current := map[string]model.AvailabilityState{"cups.service": model.StateUp}
	samples, err := BuildStateFill(context.Background(), []string{"cups.service"}, start, end, nil, time.Minute, current)
	if err != nil {
		t.Fatalf("BuildStateFill: %v", err)
	}
	if len(samples) == 0 {
		t.Fatal("slot produced no continuity samples")
	}
	for _, sample := range samples {
		if sample.State != model.StateUp {
			t.Fatalf("sample at %d is %s, want up for the whole slot", sample.TimestampUnixMS, sample.State)
		}
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
