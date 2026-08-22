package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type WAL struct {
	mu    sync.Mutex
	file  *os.File
	fsync bool
}

func Open(dir string, fsync bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	return &WAL{file: f, fsync: fsync}, nil
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
	if w.file == nil { return nil }
	return w.file.Close()
}

// ReadAll is intended for recovery/replay. It is deliberately simple for v1;
// segmented WAL and checkpointing will be added before production use.
func ReadAll(path string) ([]model.Event, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
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
	if err := s.Err(); err != nil { return nil, err }
	return events, nil
}
