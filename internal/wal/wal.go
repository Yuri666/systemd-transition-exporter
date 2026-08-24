package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	fsync  bool
	dir    string
	states map[string]model.ServiceState
}

func Open(dir string, fsync bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	w := &WAL{file: f, fsync: fsync, dir: dir, states: make(map[string]model.ServiceState)}
	stateData, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err == nil {
		if err := json.Unmarshal(stateData, &w.states); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decode WAL state: %w", err)
		}
	} else if !os.IsNotExist(err) {
		_ = f.Close()
		return nil, fmt.Errorf("read WAL state: %w", err)
	}
	return w, nil
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

// ReadAll is intended for recovery/replay. It is deliberately simple for v1;
// segmented WAL and checkpointing will be added before production use.
func ReadAll(path string) ([]model.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var events []model.Event
	s := bufio.NewScanner(f)
	for s.Scan() {
		var e model.Event
		if err := json.Unmarshal(s.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("decode WAL record: %w", err)
		}
		events = append(events, e)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
