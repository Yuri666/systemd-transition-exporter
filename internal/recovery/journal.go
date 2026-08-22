package recovery

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type journalRecord struct {
	Message string `json:"MESSAGE"`
	Unit    string `json:"_SYSTEMD_UNIT"`
	BootID  string `json:"_BOOT_ID"`
	RTUS    string `json:"__REALTIME_TIMESTAMP"`
}

// Recover reads systemd's journal for configured units. It is used only after
// a D-Bus monitoring gap. journalctl JSON records carry microsecond realtime
// timestamps, so recovered transitions retain millisecond precision in the
// exported Event model.
func Recover(ctx context.Context, services []string, from, to time.Time) ([]model.Event, error) {
	if to.Before(from) {
		return nil, nil
	}
	var out []model.Event
	for _, service := range services {
		events, err := recoverUnit(ctx, service, from, to)
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}
	// Recovery is merged with events from multiple units, therefore ordering is
	// required before the engine assigns durable sequence numbers.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].EventTimeUnixMS < out[i].EventTimeUnixMS {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

func recoverUnit(ctx context.Context, service string, from, to time.Time) ([]model.Event, error) {
	args := []string{"-u", service, "--since", fmt.Sprintf("@%d", from.Unix()), "--until", fmt.Sprintf("@%d", to.Unix()), "-o", "json", "--no-pager", "--quiet"}
	cmd := exec.CommandContext(ctx, "journalctl", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil { return nil, err }
	if err := cmd.Start(); err != nil { return nil, fmt.Errorf("journalctl %s: %w", service, err) }

	var out []model.Event
	scanner := bufio.NewScanner(stdout)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		var r journalRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil { continue }
		state, ok := messageState(r.Message)
		if !ok || r.Unit != "" && r.Unit != service { continue }
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
	if err := scanner.Err(); err != nil { _ = cmd.Process.Kill(); return nil, err }
	if err := cmd.Wait(); err != nil { return nil, fmt.Errorf("journalctl %s: %w", service, err) }
	return out, nil
}

func messageState(message string) (model.AvailabilityState, bool) {
	m := strings.ToLower(strings.TrimSpace(message))
	switch {
	case strings.HasPrefix(m, "started ") || strings.HasPrefix(m, "starting ") && strings.Contains(m, "started"):
		return model.StateUp, true
	case strings.HasPrefix(m, "stopped ") || strings.HasPrefix(m, "failed to start ") || strings.HasPrefix(m, "failed "):
		return model.StateDown, true
	default:
		return model.StateUnknown, false
	}
}

func parseTimestampUS(s string) int64 {
	var v int64
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil || v <= 0 { return 0 }
	return v
}
