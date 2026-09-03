package remote_write

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

func TestSendRetriesAfterRemoteWriteEndpointBecomesAvailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	url := server.URL
	server.Close()

	checkpoint := t.TempDir() + "/checkpoint.json"
	s := testSender(t, url, checkpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Send(ctx, []model.Event{event(1, "cups.service", model.StateUp, 1000)}); err == nil {
		t.Fatal("Send unexpectedly succeeded while endpoint was unavailable")
	}
	if got := s.LastSent(); got != 0 {
		t.Fatalf("LastSent = %d, want 0 while endpoint is unavailable", got)
	}
}

func TestConfiguredLabelsAreIncludedInRemoteWrite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wr := decodeWriteRequest(t, r)
		if len(wr.Timeseries) != 1 {
			t.Fatalf("timeseries = %d, want 1", len(wr.Timeseries))
		}
		labels := map[string]string{}
		for _, label := range wr.Timeseries[0].Labels {
			labels[label.Name] = label.Value
		}
		if labels["name"] != "cups.service" {
			t.Errorf("name label = %q, want cups.service", labels["name"])
		}
		if _, ok := labels["service"]; ok {
			t.Errorf("unexpected service label %q; unit identifier must use name", labels["service"])
		}
		if labels["site"] != "test" {
			t.Errorf("site label = %q, want test", labels["site"])
		}
		if labels["__name__"] != "systemd_service_state" {
			t.Errorf("metric name = %q, want systemd_service_state", labels["__name__"])
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	checkpoint := t.TempDir() + "/checkpoint.json"
	s := testSender(t, server.URL, checkpoint)
	if err := s.Send(context.Background(), []model.Event{event(1, "cups.service", model.StateUp, 1000)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
}
