package recovery

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

const (
	// SlotEndFraction keeps a millisecond fraction in the slot-edge timestamps.
	// A sample landing on a whole second is rendered without a decimal part,
	// and consumers that require fractional seconds skip it.
	SlotEndFraction = 5 * time.Millisecond

	// SlotEndLead is how long before the slot boundary the current state is
	// published so a range query aligned to the slot still sees a sample
	// inside [start, end) rather than on the next slot's start.
	SlotEndLead = time.Second - SlotEndFraction
)

// SlotStart returns the beginning of the local-time slot containing t. The grid
// is anchored at each hour, so a 15m window yields HH:00, HH:15, HH:30, HH:45.
func SlotStart(t time.Time, size time.Duration) time.Time {
	hour := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	return hour.Add(time.Duration(t.Minute()) * time.Minute / size * size)
}

// SlotOpeningTime is SlotEndFraction after the slot start: for a 15m grid it
// is HH:00:00.005, HH:15:00.005, and so on.
func SlotOpeningTime(slotStart time.Time) time.Time {
	return slotStart.Add(SlotEndFraction)
}

// SlotClosingTime is one second before the end of the slot that begins at
// slotStart, shifted by SlotEndFraction: for a 15m grid it is HH:14:59.005.
func SlotClosingTime(slotStart time.Time, size time.Duration) time.Time {
	return slotStart.Add(size - SlotEndLead)
}

// DueSlotOpening returns the opening timestamp of the slot containing now when
// that instant has already been reached and the slot is still open. A zero
// Time means the opener has not fallen due yet.
func DueSlotOpening(now time.Time, size time.Duration) time.Time {
	if size <= SlotEndLead {
		return time.Time{}
	}
	start := SlotStart(now, size)
	opening := SlotOpeningTime(start)
	if !now.Before(opening) && now.Before(start.Add(size)) {
		return opening
	}
	return time.Time{}
}

// NextSlotOpening is the next opening instant strictly after now, or the
// current slot's opening when it is still in the future.
func NextSlotOpening(now time.Time, size time.Duration) time.Time {
	if size <= SlotEndLead {
		return time.Time{}
	}
	start := SlotStart(now, size)
	opening := SlotOpeningTime(start)
	if now.Before(opening) {
		return opening
	}
	return SlotOpeningTime(start.Add(size))
}

// DueSlotClosing returns the closing timestamp of the slot containing now when
// that instant has already been reached and the slot is still open. A zero
// Time means the closer has not fallen due yet.
func DueSlotClosing(now time.Time, size time.Duration) time.Time {
	if size <= SlotEndLead {
		return time.Time{}
	}
	start := SlotStart(now, size)
	closing := SlotClosingTime(start, size)
	if !now.Before(closing) && now.Before(start.Add(size)) {
		return closing
	}
	return time.Time{}
}

// NextSlotClosing is the next closing instant strictly after now, or the
// current slot's closing when it is still in the future.
func NextSlotClosing(now time.Time, size time.Duration) time.Time {
	if size <= SlotEndLead {
		return time.Time{}
	}
	start := SlotStart(now, size)
	closing := SlotClosingTime(start, size)
	if now.Before(closing) {
		return closing
	}
	return SlotClosingTime(start.Add(size), size)
}

// RecoveryWindow returns only the current local-time slot [slot start, ready).
// The beginning of the observed outage is deliberately not used to widen the
// journal query into already closed slots.
func RecoveryWindow(ready time.Time, size time.Duration) (time.Time, time.Time, bool) {
	if size <= 0 {
		return time.Time{}, time.Time{}, false
	}
	start := SlotStart(ready, size)
	if !ready.After(start) {
		return time.Time{}, time.Time{}, false
	}
	return start, ready, true
}

// BuildStateFill reconstructs the state timeline of a recovered slot. Prometheus
// drops a series from instant queries once no sample is newer than its lookback
// delta, so an exporter outage must be republished as samples rather than left
// to interpolation. Samples are emitted at every interval tick inside the slot
// and at every recovered transition, so the slot is continuous while transitions
// keep their exact timestamps.
//
// The state at the slot start is derived from the recovered transitions and
// the currently observed state; see slotStartState. Nothing before the slot
// start is ever generated: that interval is reported as uncovered instead.
// The slot-edge samples (start+5ms and end−995ms) are a live publication
// only; recovery does not invent them.
func BuildStateFill(ctx context.Context, services []string, start, end time.Time, events []model.Event, interval time.Duration, current map[string]model.AvailabilityState) ([]model.StateSample, error) {
	if interval <= 0 || !end.After(start) {
		return nil, nil
	}
	byService := make(map[string][]model.Event)
	for _, event := range events {
		if event.EventTimeUnixMS < start.UnixMilli() || event.EventTimeUnixMS >= end.UnixMilli() {
			continue
		}
		byService[event.Service] = append(byService[event.Service], event)
	}
	for service := range byService {
		sort.SliceStable(byService[service], func(i, j int) bool {
			return byService[service][i].EventTimeUnixMS < byService[service][j].EventTimeUnixMS
		})
	}

	var out []model.StateSample
	for _, service := range services {
		serviceEvents := byService[service]
		state, err := slotStartState(ctx, service, start, serviceEvents, current)
		if err != nil {
			return nil, err
		}

		eventIndex := 0
		for ts := start; ts.Before(end); ts = ts.Add(interval) {
			for eventIndex < len(serviceEvents) && serviceEvents[eventIndex].EventTimeUnixMS <= ts.UnixMilli() {
				state = serviceEvents[eventIndex].State
				out = append(out, model.StateSample{Service: service, State: state, TimestampUnixMS: serviceEvents[eventIndex].EventTimeUnixMS})
				eventIndex++
			}
			out = append(out, model.StateSample{Service: service, State: state, TimestampUnixMS: ts.UnixMilli()})
		}
		for ; eventIndex < len(serviceEvents); eventIndex++ {
			state = serviceEvents[eventIndex].State
			out = append(out, model.StateSample{Service: service, State: state, TimestampUnixMS: serviceEvents[eventIndex].EventTimeUnixMS})
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].TimestampUnixMS == out[j].TimestampUnixMS {
			return out[i].Service < out[j].Service
		}
		return out[i].TimestampUnixMS < out[j].TimestampUnixMS
	})
	return out, nil
}

// slotStartState determines the availability at the beginning of the slot.
// systemd reports a start only for a unit that was not active, so the state
// preceding the first transition inside the slot is that transition inverted.
// Without any transition the state cannot have changed during the slot, which
// makes the currently observed state authoritative. This matters most on a
// first start with an empty WAL: a unit running since long before the slot has
// no lifecycle record to find, and assuming DOWN would backfill the slot with
// zeros for a service that was up the whole time. The journal is consulted only
// when the current state is unknown.
func slotStartState(ctx context.Context, service string, start time.Time, events []model.Event, current map[string]model.AvailabilityState) (model.AvailabilityState, error) {
	if len(events) > 0 {
		return invertState(events[0].State), nil
	}
	if state, ok := current[service]; ok && state != model.StateUnknown {
		return state, nil
	}
	state, found, err := previousState(ctx, service, start)
	if err != nil {
		return model.StateUnknown, err
	}
	if !found {
		return model.StateDown, nil
	}
	return state, nil
}

func invertState(state model.AvailabilityState) model.AvailabilityState {
	if state == model.StateUp {
		return model.StateDown
	}
	return model.StateUp
}

func previousState(ctx context.Context, service string, at time.Time) (model.AvailabilityState, bool, error) {
	args := []string{
		"-u", service,
		"--until", journalctlTimestamp(at),
		"-o", "json",
		"--no-pager",
		"--quiet",
		"--reverse",
		"-n", "1",
		"MESSAGE_ID=" + messageIDUnitStarted,
		"MESSAGE_ID=" + messageIDUnitStopped,
		"MESSAGE_ID=" + messageIDUnitFailed,
	}
	cmd := exec.CommandContext(ctx, "journalctl", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return model.StateUnknown, false, err
	}
	if err := cmd.Start(); err != nil {
		return model.StateUnknown, false, fmt.Errorf("journalctl %s: %w", service, err)
	}

	var foundState model.AvailabilityState
	found := false
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		var r journalRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			continue
		}
		state, ok := messageState(r.MessageID, r.Message)
		if !ok || (r.Unit != "" && r.Unit != service) {
			continue
		}
		us := parseTimestampUS(r.RTUS)
		if us == 0 || us/1000 >= at.UnixMilli() {
			continue
		}
		foundState = state
		found = true
		break
	}
	if err := scanner.Err(); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return model.StateUnknown, false, err
	}
	waitErr := cmd.Wait()
	if found {
		return foundState, true, nil
	}
	// journalctl exits with 1 when the filter matches nothing, which for a
	// baseline lookup only means the unit has no earlier lifecycle record.
	if waitErr != nil && exitCode(waitErr) != 1 {
		return model.StateUnknown, false, waitErr
	}
	return model.StateUnknown, false, nil
}
