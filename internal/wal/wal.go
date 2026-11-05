package wal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

// Linux ENOSPC is 28 and EDQUOT is 122 on the architectures this exporter runs.
// The numeric values keep the check portable for tests on other systems.
const (
	noSpaceErrno   = syscall.Errno(28)
	diskQuotaErrno = syscall.Errno(122)
)

// openWritable creates the temporary state file. Tests replace it to simulate
// a filesystem that cannot accept another write.
var openWritable = func(name string) (syncFile, error) {
	return os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
}

type syncFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type walFile interface {
	Write([]byte) (int, error)
	Sync() error
	Seek(int64, int) (int64, error)
	Truncate(int64) error
	Close() error
}

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
	mu         sync.Mutex
	file       walFile
	fsync      bool
	dir        string
	states     map[string]model.ServiceState
	repaired   Report
	pending    []model.Event
	stateDirty bool
	syncNeeded bool
	blocked    bool
	lastLog    time.Time
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
	if err := w.flushLocked(); err != nil {
		if !isNoSpace(err) {
			return err
		}
		w.remember(event)
		w.block(err)
		return nil
	}
	if err := w.writeEventLocked(event); err != nil {
		if !isNoSpace(err) {
			return err
		}
		// A failed sync means the record is already in the file. Queueing it
		// again would duplicate the line once the disk accepts writes.
		if !w.syncNeeded {
			w.remember(event)
		}
		w.block(err)
		return nil
	}
	w.unblock()
	return nil
}

// Writable reports whether the last WAL write reached the disk. Pending events
// stay in memory until a later Append or SaveState can persist them.
func (w *WAL) Writable() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.blocked
}

// Pending is the number of transition events accepted while the disk could not
// store them.
func (w *WAL) Pending() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}

func (w *WAL) remember(event model.Event) {
	w.pending = append(w.pending, event)
}

func (w *WAL) block(err error) {
	w.blocked = true
	if time.Since(w.lastLog) < time.Minute {
		return
	}
	w.lastLog = time.Now()
	log.Printf("WAL write deferred pending_events=%d: %v", len(w.pending), err)
}

func (w *WAL) unblock() {
	if !w.blocked {
		return
	}
	w.blocked = false
	log.Printf("WAL writable again")
}

func (w *WAL) flushLocked() error {
	if w.syncNeeded {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync WAL: %w", err)
		}
		w.syncNeeded = false
	}
	for len(w.pending) > 0 {
		err := w.writeEventLocked(w.pending[0])
		if w.syncNeeded {
			w.pending = w.pending[1:]
		}
		if err != nil {
			return err
		}
		if !w.syncNeeded {
			w.pending = append([]model.Event(nil), w.pending[1:]...)
		}
	}
	if w.stateDirty {
		if err := w.writeStateLocked(); err != nil {
			return err
		}
		w.stateDirty = false
	}
	return nil
}

func (w *WAL) writeEventLocked(event model.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := w.appendRecord(append(data, '\n')); err != nil {
		return err
	}
	if w.fsync {
		if err := w.file.Sync(); err != nil {
			w.syncNeeded = true
			return fmt.Errorf("sync WAL: %w", err)
		}
	}
	return nil
}

// appendRecord writes one complete line. A short or failed write is rolled
// back to the previous end of the file so the next attempt cannot merge into
// a partial line.
func (w *WAL) appendRecord(payload []byte) error {
	offset, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek WAL: %w", err)
	}
	n, err := w.file.Write(payload)
	if err != nil || n != len(payload) {
		if n > 0 {
			_ = w.file.Truncate(offset)
		}
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("write WAL: %w", err)
	}
	return nil
}

func isNoSpace(err error) bool {
	return errors.Is(err, noSpaceErrno) || errors.Is(err, diskQuotaErrno)
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
	w.stateDirty = true
	if err := w.flushLocked(); err != nil {
		if !isNoSpace(err) {
			return err
		}
		w.block(err)
		return nil
	}
	w.unblock()
	return nil
}

func (w *WAL) writeStateLocked() error {
	data, err := json.Marshal(w.states)
	if err != nil {
		return fmt.Errorf("encode WAL state: %w", err)
	}
	path := filepath.Join(w.dir, "state.json")
	tmp := path + ".tmp"
	f, err := openWritable(tmp)
	if err != nil {
		return fmt.Errorf("open WAL state temporary file: %w", err)
	}
	payload := append(data, '\n')
	n, err := f.Write(payload)
	if err != nil || n != len(payload) {
		_ = f.Close()
		_ = os.Remove(tmp)
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("write WAL state: %w", err)
	}
	if w.fsync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("sync WAL state: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
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
