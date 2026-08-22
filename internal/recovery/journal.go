package recovery

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type journalRecord struct {
	Message string `json:"MESSAGE"`
	Unit string `json:"_SYSTEMD_UNIT"`
	BootID string `json:"_BOOT_ID"`
	RTUS string `json:"__REALTIME_TIMESTAMP"`
}

// Recover reads systemd's journal for configured units during a D-Bus gap or
// exporter downtime. Use absolute RFC3339 timestamps for journalctl so the
// requested recovery window is explicit and is not reduced to whole seconds.
func Recover(ctx context.Context, services []string, from, to time.Time) ([]model.Event, error) {
	if to.Before(from) { return nil, nil }
	var out []model.Event
	for _, service := range services {
		events, err := recoverUnit(ctx, service, from, to)
		if err != nil { return nil, err }
		out = append(out, events...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EventTimeUnixMS == out[j].EventTimeUnixMS { return out[i].Service < out[j].Service }
		return out[i].EventTimeUnixMS < out[j].EventTimeUnixMS
	})
	return out, nil
}

func recoverUnit(ctx context.Context, service string, from, to time.Time) ([]model.Event, error) {
	args := []string{
		"-u", service,
		"--since", from.Format(time.RFC3339Nano),
		"--until", to.Format(time.RFC3339Nano),
		"-o", "json",
		"--no-pager",
		"--quiet",
	}
	cmd := exec.CommandContext(ctx, "journalctl", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil { return nil, err }
	if err := cmd.Start(); err != nil { return nil, fmt.Errorf("journalctl %s: %w", service, err) }

	var out []model.Event
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		var r journalRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil { continue }
		state, ok := messageState(r.Message)
		if !ok { continue }
		if r.Unit != "" && r.Unit != service { continue }
		us := parseTimestampUS(r.RTUS)
		if us == 0 { continue }
		t := time.Unix(0, us*1000)
		if t.Before(from) || t.After(to) { continue }
		out = append(out, model.Event{
			Service: service,
			State: state,
			EventTimeUnixMS: us / 1000,
			BootID: r.BootID,
			Source: model.SourceRecovery,
			SystemdActiveState: state.String(),
		})
	}
	if err := scanner.Err(); err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}
	if err := cmd.Wait(); err != nil { return nil, fmt.Errorf("journalctl %s: %w", service, err) }
	return out, nil
}

func messageState(message string) (model.AvailabilityState, bool) {
	m := strings.ToLower(strings.TrimSpace(message))
	switch {
	case strings.HasPrefix(m, "started "):
		return model.StateUp, true
	case strings.HasPrefix(m, "stopped "), strings.HasPrefix(m, "failed to start "):
		return model.StateDown, true
	default:
		return model.StateUnknown, false
	}
}

func parseTimestampUS(s string) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || v <= 0 { return 0 }
	return v
}
