package wal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

func TestReadAllDropsInterruptedTrailingRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"sequence":1,"service":"pcscf.service"}`+"\n"+`{"sequence":2,"serv`), 0640); err != nil {
		t.Fatal(err)
	}

	events, report, err := ReadAll(path)
	if err != nil {
		t.Fatalf("an interrupted trailing record must not fail the replay: %v", err)
	}
	if len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("got %+v, want only the complete record", events)
	}
	if report.TruncatedBytes == 0 {
		t.Fatal("the interrupted record was not reported")
	}
}

func TestReadAllSkipsUndecodableRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"sequence":1}`+"\n\x00\x00\x00\n"+`{"sequence":2}`+"\n"), 0640); err != nil {
		t.Fatal(err)
	}

	events, report, err := ReadAll(path)
	if err != nil {
		t.Fatalf("a damaged record must not fail the replay: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want the 2 decodable ones", len(events))
	}
	if report.SkippedRecords != 1 {
		t.Fatalf("got %d skipped records, want 1", report.SkippedRecords)
	}
}

func TestOpenTruncatesInterruptedRecordSoNextAppendStaysReadable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"sequence":1}`+"\n"+`{"seq`), 0640); err != nil {
		t.Fatal(err)
	}

	w, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if report := w.Repaired(); report.TruncatedBytes == 0 {
		t.Fatal("Open did not report the interrupted record")
	}
	if err := w.Append(model.Event{Sequence: 2, Service: "pcscf.service"}); err != nil {
		t.Fatal(err)
	}

	events, report, err := ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Empty() {
		t.Fatalf("the repaired log still reports damage: %+v", report)
	}
	if len(events) != 2 || events[1].Sequence != 2 {
		t.Fatalf("got %+v, want the appended record after the repaired one", events)
	}
}

func TestOpenStartsWithEmptyStateWhenStateFileIsDamaged(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"pcscf.servi`), 0640); err != nil {
		t.Fatal(err)
	}

	w, err := Open(dir, true)
	if err != nil {
		t.Fatalf("a damaged state file must not prevent startup: %v", err)
	}
	defer w.Close()
	if !w.Repaired().StateReset {
		t.Fatal("the discarded state was not reported")
	}
	if states := w.States(); len(states) != 0 {
		t.Fatalf("got %+v, want no restored state", states)
	}
}

func TestStateSurvivesReopenWithoutTransitionEvents(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	want := model.ServiceState{
		Service:      "pcscf.service",
		Availability: model.StateUp,
		ActiveState:  "active",
		BootID:       "boot-1",
	}
	if err := w.SaveState(want); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	states := reopened.States()
	if len(states) != 1 {
		t.Fatalf("got %d persisted states, want 1", len(states))
	}
	if states[0].Service != want.Service || states[0].Availability != want.Availability || states[0].BootID != want.BootID {
		t.Fatalf("unexpected persisted state: %+v", states[0])
	}
}

func TestAppendDefersWhenDiskIsFullThenFlushesInOrder(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.file.Close(); err != nil {
		t.Fatal(err)
	}
	store := &memWAL{fail: noSpaceErrno}
	w.file = store

	first := model.Event{Sequence: 1, Service: "scscf.service"}
	second := model.Event{Sequence: 2, Service: "icscf.service"}
	if err := w.Append(first); err != nil {
		t.Fatalf("a full disk must not fail Append: %v", err)
	}
	if err := w.Append(second); err != nil {
		t.Fatal(err)
	}
	if w.Writable() || w.Pending() != 2 {
		t.Fatalf("writable=%t pending=%d, want blocked with 2 events", w.Writable(), w.Pending())
	}
	if len(store.buf) != 0 {
		t.Fatalf("partial WAL write was left behind: %q", store.buf)
	}

	store.fail = nil
	third := model.Event{Sequence: 3, Service: "pcscf.service"}
	if err := w.Append(third); err != nil {
		t.Fatal(err)
	}
	if !w.Writable() || w.Pending() != 0 {
		t.Fatalf("writable=%t pending=%d after the disk accepted writes", w.Writable(), w.Pending())
	}
	events := decodeWAL(t, store.buf)
	if len(events) != 3 || events[0].Sequence != 1 || events[1].Sequence != 2 || events[2].Sequence != 3 {
		t.Fatalf("got %+v, want events 1, 2, 3 in order", events)
	}
}

func TestAppendStillReturnsOtherWriteErrors(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.file.Close(); err != nil {
		t.Fatal(err)
	}
	w.file = &memWAL{fail: os.ErrPermission}
	if err := w.Append(model.Event{Sequence: 1, Service: "scscf.service"}); err == nil {
		t.Fatal("a permission error was treated as a full disk")
	}
	if !w.Writable() || w.Pending() != 0 {
		t.Fatalf("writable=%t pending=%d, want the WAL to stay writable with nothing queued", w.Writable(), w.Pending())
	}
}

func TestSyncFailureDoesNotDuplicateTheRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	failing := &syncFailFile{File: w.file.(*os.File), fail: true}
	w.file = failing
	if err := w.Append(model.Event{Sequence: 1, Service: "scscf.service"}); err != nil {
		t.Fatal(err)
	}
	if w.Pending() != 0 {
		t.Fatalf("pending=%d, the record is already in the file and must not be queued again", w.Pending())
	}
	failing.fail = false
	if err := w.Append(model.Event{Sequence: 2, Service: "icscf.service"}); err != nil {
		t.Fatal(err)
	}
	events, _, err := ReadAll(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("got %+v, want each event once", events)
	}
}

func TestSaveStateDefersWhenDiskIsFull(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	previous := openWritable
	t.Cleanup(func() { openWritable = previous })
	openWritable = func(string) (syncFile, error) {
		return rejectWriter{err: noSpaceErrno}, nil
	}
	want := model.ServiceState{Service: "scscf.service", Availability: model.StateUp, ActiveState: "active"}
	if err := w.SaveState(want); err != nil {
		t.Fatalf("a full disk must not fail SaveState: %v", err)
	}
	if w.Writable() {
		t.Fatal("WAL was reported writable after state.json could not be written")
	}
	states := w.States()
	if len(states) != 1 || states[0].Service != want.Service || states[0].Availability != want.Availability {
		t.Fatalf("in-memory state = %+v", states)
	}

	openWritable = previous
	if err := w.SaveState(want); err != nil {
		t.Fatal(err)
	}
	if !w.Writable() {
		t.Fatal("WAL stayed blocked after state.json was written")
	}
	reopened, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored := reopened.States()
	if len(restored) != 1 || restored[0].Service != want.Service {
		t.Fatalf("restored state = %+v", restored)
	}
}

type memWAL struct {
	buf  []byte
	fail error
}

func (m *memWAL) Write(p []byte) (int, error) {
	if m.fail != nil {
		n := 4
		if n > len(p) {
			n = len(p)
		}
		m.buf = append(m.buf, p[:n]...)
		return n, m.fail
	}
	m.buf = append(m.buf, p...)
	return len(p), nil
}

func (m *memWAL) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekEnd || offset != 0 {
		return 0, fmt.Errorf("unexpected seek %d %d", offset, whence)
	}
	return int64(len(m.buf)), nil
}

func (m *memWAL) Truncate(size int64) error {
	m.buf = m.buf[:size]
	return nil
}

func (m *memWAL) Sync() error  { return nil }
func (m *memWAL) Close() error { return nil }

func decodeWAL(t *testing.T, buf []byte) []model.Event {
	t.Helper()
	var events []model.Event
	for _, line := range bytes.Split(bytes.TrimSuffix(buf, []byte("\n")), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var event model.Event
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		events = append(events, event)
	}
	return events
}

type syncFailFile struct {
	*os.File
	fail bool
}

func (f *syncFailFile) Sync() error {
	if f.fail {
		return noSpaceErrno
	}
	return f.File.Sync()
}

type rejectWriter struct {
	err error
}

func (r rejectWriter) Write([]byte) (int, error) { return 0, r.err }
func (r rejectWriter) Sync() error               { return nil }
func (r rejectWriter) Close() error              { return nil }
