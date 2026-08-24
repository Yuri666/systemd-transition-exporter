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

// RecoveryWindow returns aligned recovery windows covering [from, to].
// The grid is anchored at the start of every hour, so a configured window
// always starts at HH:00, HH:<N>, ... and resets at the next hour.
func RecoveryWindow(from, to time.Time, size time.Duration) [][2]time.Time {
	if size <= 0 || !to.After(from) {
		return nil
	}
	var out [][2]time.Time
	hour := time.Date(from.Year(), from.Month(), from.Day(), from.Hour(), 0, 0, 0, from.Location())
	for !hour.After(to) {
		for offset := time.Duration(0); offset < time.Hour; offset += size {
			start := hour.Add(offset)
			if start.Before(from) && start.Add(size).Before(from) {
				continue
			}
			if start.After(to) {
				break
			}
			end := start.Add(size)
			if end.After(hour.Add(time.Hour)) {
				end = hour.Add(time.Hour)
			}
			if end.After(to) {
				end = to
			}
			if end.After(start) {
				out = append(out, [2]time.Time{start, end})
			}
		}
		hour = hour.Add(time.Hour)
	}
	return out
}

// BuildStateBackfill creates timestamped state samples at interval-aligned
// points inside a recovered window. The state at the beginning of the window
// is taken from the latest journal transition at or before the window start.
// Each sample represents the state effective at that timestamp.
func BuildStateBackfill(ctx context.Context, services []string, start, end time.Time, events []model.Event, interval time.Duration) ([]model.StateSample, error) {
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
			continue
		}

		serviceEvents := byService[service]
		eventIndex := 0
		for ts := start; ts.Before(end); ts = ts.Add(interval) {
			for eventIndex < len(serviceEvents) && serviceEvents[eventIndex].EventTimeUnixMS <= ts.UnixMilli() {
				state = serviceEvents[eventIndex].State
				eventIndex++
			}
			out = append(out, model.StateSample{Service: service, State: state, TimestampUnixMS: ts.UnixMilli()})
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
		"-n", "50",
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
		if us == 0 || us/1000 > uint64(at.UnixMilli()) {
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
	if err := cmd.Wait(); err != nil && exitCode(err) != 1 {
		return model.StateUnknown, false, fmt.Errorf("journalctl %s: %w", service, err)
	}
	return foundState, found, nil
}
