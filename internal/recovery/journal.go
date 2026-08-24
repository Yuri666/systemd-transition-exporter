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

// These MESSAGE_ID values are the systemd journal message identifiers for
// the actual unit state transitions. They are deliberately different from
// the "Starting"/"Stopping" progress messages.
const (
	messageIDUnitStarted = "39f53479d3a045ac8e11786248231fbf"
	messageIDUnitStopped = "9d1aaa27d60140bd96365438aad20286"
	messageIDUnitFailed  = "7d4958e842da4a758f6c1cdc7b36dcc5"
)

type journalRecord struct {
	Message     string `json:"MESSAGE"`
	MessageID   string `json:"MESSAGE_ID"`
	SystemdUnit string `json:"_SYSTEMD_UNIT"`
	Unit        string `json:"UNIT"`
	BootID      string `json:"_BOOT_ID"`
	RTUS        string `json:"__REALTIME_TIMESTAMP"`
}

// Recover reads systemd's journal for configured units during a D-Bus gap or
// exporter downtime. Transition detection uses systemd's MESSAGE_ID fields
// rather than localized human-readable MESSAGE text.
func Recover(ctx context.Context, services []string, from, to time.Time) ([]model.Event, error) {
	if to.Before(from) {
		return nil, nil
	}
	var out []model.Event
	for _, service := range services {
		events, err := recoverUnit(ctx, service, from, to)
		if err != nil {
			if isNoJournalEntriesError(err) {
				continue
			}
			return nil, err
		}
		out = append(out, events...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EventTimeUnixMS == out[j].EventTimeUnixMS {
			return out[i].Service < out[j].Service
		}
		return out[i].EventTimeUnixMS < out[j].EventTimeUnixMS
	})
	return out, nil
}

func recoverUnit(ctx context.Context, service string, from, to time.Time) ([]model.Event, error) {
	args := []string{
		"-u", service,
		"--since", journalctlTimestamp(from),
		"--until", journalctlTimestamp(to),
		"-o", "json",
		"--no-pager",
		"--quiet",
	}
	cmd := exec.CommandContext(ctx, "journalctl", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("journalctl %s: %w", service, err)
	}

	var out []model.Event
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		var r journalRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			continue
		}
		state, ok := messageState(r.MessageID, r.Message)
		if !ok {
			continue
		}

		// journalctl -u service is already the authoritative journal filter.
		// For lifecycle records, UNIT contains the affected unit while
		// _SYSTEMD_UNIT may contain the emitting manager scope (e.g. init.scope).
		// Do not filter on _SYSTEMD_UNIT. If UNIT is present, use it to reject
		// records that demonstrably belong to another unit.
		if r.Unit != "" && r.Unit != service {
			continue
		}

		us := parseTimestampUS(r.RTUS)
		if us == 0 {
			continue
		}
		t := time.Unix(0, us*1000)
		if t.Before(from) || !t.Before(to) {
			continue
		}
		out = append(out, model.Event{
			Service:            service,
			State:              state,
			EventTimeUnixMS:    us / 1000,
			BootID:             r.BootID,
			Source:             model.SourceRecovery,
			SystemdActiveState: state.String(),
		})
	}
	if err := scanner.Err(); err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}

	stderrBytes, _ := readAll(stderr)
	if err := cmd.Wait(); err != nil {
		if exitCode(err) == 1 {
			return nil, fmt.Errorf("journalctl %s: no journal entries: %w", service, err)
		}
		if len(stderrBytes) > 0 {
			return nil, fmt.Errorf("journalctl %s: %s: %w", service, strings.TrimSpace(string(stderrBytes)), err)
		}
		return nil, fmt.Errorf("journalctl %s: %w", service, err)
	}
	return out, nil
}

// journalctlTimestamp deliberately uses journalctl's portable local-time
// syntax instead of RFC3339 with an explicit offset. Some systemd/journalctl
// versions reject values such as "2026-08-22T20:14:39.677+03:00" even though
// they are valid RFC3339 timestamps. The journal record's own
// __REALTIME_TIMESTAMP remains the authoritative event timestamp.
func journalctlTimestamp(t time.Time) string {
	return t.Local().Format("2006-01-02 15:04:05")
}

func isNoJournalEntriesError(err error) bool {
	return strings.Contains(err.Error(), ": no journal entries:")
}

func exitCode(err error) int {
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	return -1
}

func readAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var b []byte
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			b = append(b, buf[:n]...)
		}
		if err != nil {
			return b, err
		}
	}
}

func messageState(messageID, message string) (model.AvailabilityState, bool) {
	switch strings.ToLower(strings.TrimSpace(messageID)) {
	case messageIDUnitStarted:
		return model.StateUp, true
	case messageIDUnitStopped, messageIDUnitFailed:
		return model.StateDown, true
	}

	// Fallback for journal implementations that do not expose MESSAGE_ID.
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
	if err != nil || v <= 0 {
		return 0
	}
	return v
}
