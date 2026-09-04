package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
	"github.com/Yuri666/systemd-transition-exporter/internal/remote_write"
)

func newTestWorker(t *testing.T, ctx context.Context, id, url string) (*Worker, *remote_write.Sender) {
	t.Helper()
	sender, err := remote_write.New(remote_write.Config{
		Enabled:       true,
		URL:           url,
		BatchSize:     10,
		FlushInterval: 10 * time.Millisecond,
		RetryInterval: time.Millisecond,
		Timeout:       100 * time.Millisecond,
		Checkpoint:    filepath.Join(t.TempDir(), id+".checkpoint"),
		StateInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := New(Config{
		TargetID:      id,
		BatchSize:     10,
		FlushInterval: 10 * time.Millisecond,
		StateInterval: time.Hour,
	}, sender)
	go worker.Run(ctx)
	return worker, sender
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestUnavailableTargetDoesNotBlockHealthyTarget(t *testing.T) {
	var healthyRequests atomic.Int32
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		healthyRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer healthy.Close()

	var failedRequests atomic.Int32
	var unavailable atomic.Bool
	unavailable.Store(true)
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		failedRequests.Add(1)
		if unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer failed.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	healthyWorker, healthySender := newTestWorker(t, ctx, "healthy", healthy.URL)
	failedWorker, failedSender := newTestWorker(t, ctx, "failed", failed.URL)
	event := model.Event{
		Sequence:        1,
		Service:         "cups.service",
		State:           model.StateDown,
		EventTimeUnixMS: time.Now().UnixMilli(),
	}
	if !healthyWorker.EnqueueEvent(event) || !failedWorker.EnqueueEvent(event) {
		t.Fatal("failed to enqueue event")
	}

	waitFor(t, func() bool { return healthySender.LastSent() == 1 })
	if healthyRequests.Load() == 0 {
		t.Fatal("healthy target received no request")
	}
	if failedSender.LastSent() != 0 {
		t.Fatalf("failed target checkpoint = %d, want 0", failedSender.LastSent())
	}
	waitFor(t, func() bool { return failedRequests.Load() > 0 })
	unavailable.Store(false)
	waitFor(t, func() bool { return failedSender.LastSent() == 1 })
}

func TestBothTargetsReceiveSameTransition(t *testing.T) {
	var firstRequests atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer first.Close()
	var secondRequests atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer second.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstWorker, firstSender := newTestWorker(t, ctx, "first", first.URL)
	secondWorker, secondSender := newTestWorker(t, ctx, "second", second.URL)
	event := model.Event{Sequence: 7, Service: "cups.service", State: model.StateUp, EventTimeUnixMS: time.Now().UnixMilli()}
	firstWorker.EnqueueEvent(event)
	secondWorker.EnqueueEvent(event)

	waitFor(t, func() bool { return firstSender.LastSent() == 7 && secondSender.LastSent() == 7 })
	if firstRequests.Load() == 0 || secondRequests.Load() == 0 {
		t.Fatalf("requests: first=%d second=%d", firstRequests.Load(), secondRequests.Load())
	}
}

func TestSlotStateSamplesUseExplicitTimestamp(t *testing.T) {
	at := time.Date(2026, 9, 4, 15, 0, 0, int(5*time.Millisecond), time.Local)
	samples := slotStateSamples([]string{"cups.service", "missing.service"}, func(service string) (model.ServiceState, bool) {
		if service != "cups.service" {
			return model.ServiceState{}, false
		}
		return model.ServiceState{Service: service, Availability: model.StateUp}, true
	}, at)
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if samples[0].TimestampUnixMS != at.UnixMilli() {
		t.Fatalf("timestamp = %d, want %d", samples[0].TimestampUnixMS, at.UnixMilli())
	}
	if samples[0].State != model.StateUp || samples[0].Service != "cups.service" {
		t.Fatalf("sample = %+v", samples[0])
	}
}
