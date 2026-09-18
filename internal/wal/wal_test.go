package wal

import (
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
