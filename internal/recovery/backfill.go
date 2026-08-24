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

// SlotStart returns the beginning of the local-time slot containing t. The grid
// is anchored at each hour, so a 15m window yields HH:00, HH:15, HH:30, HH:45.
func SlotStart(t time.Time, size time.Duration) time.Time {
	hour := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	return hour.Add(time.Duration(t.Minute()) * time.Minute / size * size)
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
// The state at the slot start comes from the latest journal transition strictly
// before the slot. A configured service with no lifecycle history is DOWN.
// Nothing before the slot start is ever generated: that interval is reported as
// uncovered instead.
func BuildStateFill(ctx context.Context, services []string, start, end time.Time, events []model.Event, interval time.Duration) ([]model.StateSample, error) {
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
		state, ok, err := previousState(ctx, service, start)
		if err != nil {
			return nil, err
		}
		if !ok {
			// The service is explicitly configured but has no lifecycle event
			// in the journal. Its Prometheus availability state is therefore
			// down until systemd reports a successful start.
			state = model.StateDown
		}

		serviceEvents := byService[service]
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
		return model.StateUnknown, false, err
	}
	if err := cmd.Wait(); err != nil {
		return model.StateUnknown, false, err
	}
	return foundState, found, nil
}
