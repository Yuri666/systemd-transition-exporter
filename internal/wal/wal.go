package wal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

// Report describes damage found in the event log at startup. A torn trailing
// record is expected rather than exceptional: an append interrupted between
// the payload and its newline leaves a partial line behind. Refusing to start
// over such a line turns one lost transition into a permanent crash loop, so
// the damage is repaired and reported instead.
type Report struct {
	SkippedRecords int
	TruncatedBytes int64
	StateReset     bool
}

func (r Report) Empty() bool {
	return r.SkippedRecords == 0 && r.TruncatedBytes == 0 && !r.StateReset
}

type WAL struct {
	mu       sync.Mutex
	file     *os.File
	fsync    bool
	dir      string
	states   map[string]model.ServiceState
	repaired Report
}

func Open(dir string, fsync bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}
	report, err := Repair(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	w := &WAL{file: f, fsync: fsync, dir: dir, states: make(map[string]model.ServiceState), repaired: report}
	stateData, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err == nil {
		if err := json.Unmarshal(stateData, &w.states); err != nil {
			// The state file is rewritten atomically, so a damaged one means
			// the previous write never completed. It is a cache of the last
			// observed state: starting without it costs a snapshot, while
			// refusing to start costs all monitoring.
			w.states = make(map[string]model.ServiceState)
			w.repaired.StateReset = true
		}
	} else if !os.IsNotExist(err) {
		_ = f.Close()
		return nil, fmt.Errorf("read WAL state: %w", err)
	}
	return w, nil
}

// Repaired reports what Open had to discard.
func (w *WAL) Repaired() Report {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.repaired
}

func (w *WAL) Append(event model.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err = w.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}
	if w.fsync {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync WAL: %w", err)
		}
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *WAL) States() []model.ServiceState {
	w.mu.Lock()
	defer w.mu.Unlock()
	states := make([]model.ServiceState, 0, len(w.states))
	for _, state := range w.states {
		states = append(states, state)
	}
	return states
}

// SaveState persists the latest observed state independently from transition
// events. It allows a new process to determine which services were UP before
// a host reboot even if no transition had ever been appended to events.jsonl.
func (w *WAL) SaveState(state model.ServiceState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.states[state.Service] = state
	data, err := json.Marshal(w.states)
	if err != nil {
		return fmt.Errorf("encode WAL state: %w", err)
	}
	path := filepath.Join(w.dir, "state.json")
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
	if err != nil {
		return fmt.Errorf("open WAL state temporary file: %w", err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		return fmt.Errorf("write WAL state: %w", err)
	}
	if w.fsync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("sync WAL state: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close WAL state: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		// Windows does not replace an existing destination. Production Linux
		// uses the atomic rename path above; this fallback keeps tests portable.
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("replace WAL state: %w", err)
		}
		if err := os.Rename(tmp, path); err != nil {
			return fmt.Errorf("rename WAL state: %w", err)
		}
	}
	if w.fsync {
		dir, err := os.Open(w.dir)
		if err != nil {
			return fmt.Errorf("open WAL state directory: %w", err)
		}
		if err := dir.Sync(); err != nil && runtime.GOOS != "windows" {
			_ = dir.Close()
			return fmt.Errorf("sync WAL state directory: %w", err)
		}
		if err := dir.Close(); err != nil {
			return fmt.Errorf("close WAL state directory: %w", err)
		}
	}
	return nil
}

// ReadAll is intended for recovery/replay. A record that cannot be decoded is
// skipped rather than fatal, and an interrupted trailing record is dropped:
// see Report.
func ReadAll(path string) ([]model.Event, Report, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, Report{}, err
	}
	defer f.Close()
	events, report, _, err := readRecords(f)
	return events, report, err
}

// Repair drops an interrupted trailing record so the next append starts at a
// record boundary. Without it the partial line and the record appended after
// it merge into one undecodable line, turning one lost transition into two.
func Repair(path string) (Report, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return Report{}, nil
	}
	if err != nil {
		return Report{}, fmt.Errorf("open WAL for repair: %w", err)
	}
	_, found, complete, err := readRecords(f)
	_ = f.Close()
	if err != nil {
		return Report{}, err
	}
	// Repair only removes the torn tail; records it merely could not decode
	// stay in the file and are reported by the replay that reads them.
	report := Report{TruncatedBytes: found.TruncatedBytes}
	if report.TruncatedBytes > 0 {
		if err := os.Truncate(path, complete); err != nil {
			return report, fmt.Errorf("truncate interrupted WAL record: %w", err)
		}
	}
	return report, nil
}

// readRecords decodes the log and returns the offset of the end of the last
// complete line, which is where a torn tail begins.
func readRecords(f *os.File) ([]model.Event, Report, int64, error) {
	reader := bufio.NewReaderSize(f, 64*1024)
	var (
		events   []model.Event
		report   Report
		complete int64
	)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if err != nil {
				// No newline terminates this line, so the writer was
				// interrupted before the record was complete.
				report.TruncatedBytes += int64(len(line))
			} else {
				payload := bytes.TrimSpace(line)
				if len(payload) > 0 {
					var event model.Event
					if json.Unmarshal(payload, &event) != nil {
						report.SkippedRecords++
					} else {
						events = append(events, event)
					}
				}
				complete += int64(len(line))
			}
		}
		if err != nil {
			if err == io.EOF {
				return events, report, complete, nil
			}
			return events, report, complete, fmt.Errorf("read WAL: %w", err)
		}
	}
}
