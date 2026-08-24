package remote_write

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

func testSender(t *testing.T, serverURL, checkpointPath string) *Sender {
	t.Helper()
	s, err := New(Config{
		Enabled:       true,
		URL:           serverURL,
		BatchSize:     2,
		RetryInterval: time.Millisecond,
		Timeout:       time.Second,
		Checkpoint:    checkpointPath,
		Labels:        map[string]string{"site": "test"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func decodeWriteRequest(t *testing.T, r *http.Request) *prompb.WriteRequest {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	decoded, err := snappy.Decode(nil, body)
	if err != nil {
		t.Fatalf("snappy decode: %v", err)
	}
	var wr prompb.WriteRequest
	if err := wr.Unmarshal(decoded); err != nil {
		t.Fatalf("unmarshal write request: %v", err)
	}
	return &wr
}

func event(seq uint64, service string, state model.AvailabilityState, ts int64) model.Event {
	return model.Event{
		Sequence:        seq,
		Service:         service,
		State:           state,
		EventTimeUnixMS: ts,
		Source:          model.SourceSystemd,
	}
}

func TestSendRetriesAfterServerError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if requests.Load() == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	s := testSender(t, server.URL, checkpointPath)
	if err := s.Send(context.Background(), []model.Event{event(1, "cups.service", model.StateDown, 1000)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
	if got := s.Stats().Retries; got != 1 {
		t.Fatalf("retries = %d, want 1", got)
	}
	if got := s.LastSent(); got != 1 {
		t.Fatalf("LastSent = %d, want 1", got)
	}
}

func TestSendDoesNotRetryClientError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	s := testSender(t, server.URL, checkpointPath)
	if err := s.Send(context.Background(), []model.Event{event(1, "cups.service", model.StateDown, 1000)}); err == nil {
		t.Fatal("Send unexpectedly succeeded")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	if got := s.LastSent(); got != 0 {
		t.Fatalf("LastSent = %d, want 0", got)
	}
}

func TestCheckpointPersistsAfterSuccessfulBatch(t *testing.T) {
	var samples atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wr := decodeWriteRequest(t, r)
		for _, ts := range wr.Timeseries {
			samples.Add(int32(len(ts.Samples)))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	s := testSender(t, server.URL, checkpointPath)
	events := []model.Event{
		event(1, "cups.service", model.StateDown, 1000),
		event(2, "cups.service", model.StateUp, 2000),
	}
	if err := s.Send(context.Background(), events); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := samples.Load(); got != 2 {
		t.Fatalf("samples = %d, want 2", got)
	}

	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	var c checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	if c.LastSequence != 2 {
		t.Fatalf("checkpoint sequence = %d, want 2", c.LastSequence)
	}

	restarted := testSender(t, server.URL, checkpointPath)
	if got := restarted.LastSent(); got != 2 {
		t.Fatalf("restarted LastSent = %d, want 2", got)
	}
}

func TestRestartResumesFromCheckpointWithoutResendingDeliveredEvents(t *testing.T) {
	var requests atomic.Int32
	var received []int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		wr := decodeWriteRequest(t, r)
		for _, ts := range wr.Timeseries {
			for _, sample := range ts.Samples {
				received = append(received, sample.Timestamp)
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	first := testSender(t, server.URL, checkpointPath)
	if err := first.Send(context.Background(), []model.Event{
		event(1, "cups.service", model.StateDown, 1000),
		event(2, "cups.service", model.StateUp, 2000),
	}); err != nil {
		t.Fatalf("first Send: %v", err)
	}

	second := testSender(t, server.URL, checkpointPath)
	if err := second.Send(context.Background(), []model.Event{
		event(1, "cups.service", model.StateDown, 1000),
		event(2, "cups.service", model.StateUp, 2000),
		event(3, "cups.service", model.StateDown, 3000),
	}); err != nil {
		t.Fatalf("second Send: %v", err)
	}

	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
	if len(received) != 3 {
		t.Fatalf("received samples = %d, want 3", len(received))
	}
	if received[0] != 1000 || received[1] != 2000 || received[2] != 3000 {
		t.Fatalf("received timestamps = %v, want [1000 2000 3000]", received)
	}
}

func TestCurrentStateDoesNotAdvanceTransitionCheckpoint(t *testing.T) {
	var gotState bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wr := decodeWriteRequest(t, r)
		if len(wr.Timeseries) != 1 || len(wr.Timeseries[0].Samples) != 1 {
			t.Fatalf("unexpected write request: %+v", wr)
		}
		for _, label := range wr.Timeseries[0].Labels {
			if label.Name == "__name__" && label.Value == "systemd_service_state" {
				gotState = true
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	s := testSender(t, server.URL, checkpointPath)
	if err := s.SendCurrentStates(context.Background(), []model.ServiceState{{
		Service:      "cups.service",
		Availability: model.StateUp,
	}}); err != nil {
		t.Fatalf("SendCurrentStates: %v", err)
	}
	if !gotState {
		t.Fatal("current state metric was not sent")
	}
	if got := s.LastSent(); got != 0 {
		t.Fatalf("LastSent = %d, want 0", got)
	}
}

func TestRemoteWritePreservesEventTimestamp(t *testing.T) {
	const eventTimestamp = int64(1787436204558)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wr := decodeWriteRequest(t, r)
		if len(wr.Timeseries) != 1 || len(wr.Timeseries[0].Samples) != 1 {
			t.Fatalf("unexpected write request: %+v", wr)
		}
		if got := wr.Timeseries[0].Samples[0].Timestamp; got != eventTimestamp {
			t.Fatalf("timestamp = %d, want %d", got, eventTimestamp)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	s := testSender(t, server.URL, checkpointPath)
	if err := s.Send(context.Background(), []model.Event{event(1, "cups.service", model.StateUp, eventTimestamp)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
}
