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

// RecoveryWindow returns only the current local-time slot [slot start, ready).
// The beginning of the observed outage is deliberately not used to widen the
// journal query into already closed slots.
func RecoveryWindow(ready time.Time, size time.Duration) (time.Time, time.Time, bool) {
	if size <= 0 {
		return time.Time{}, time.Time{}, false
	}
	local := ready
	hour := time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), 0, 0, 0, local.Location())
	start := hour.Add(time.Duration(local.Minute()) * time.Minute / size * size)
	if !ready.After(start) {
		return time.Time{}, time.Time{}, false
	}
	return start, ready, true
}

// BuildStateBaseline creates exactly one state sample per configured service
// at the beginning of the recovered slot. The state is taken from the latest
// journal transition strictly before the slot. A configured service with no
// lifecycle history is treated as DOWN.
func BuildStateBaseline(ctx context.Context, services []string, start time.Time) ([]model.StateSample, error) {
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
		out = append(out, model.StateSample{Service: service, State: state, TimestampUnixMS: start.UnixMilli()})
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
