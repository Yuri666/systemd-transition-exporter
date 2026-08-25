package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
	"github.com/Yuri666/systemd-transition-exporter/internal/remote_write"
)

func TestHandlerDoesNotExposeRemoteWriteServiceSeries(t *testing.T) {
	r := New()
	r.Event(model.Event{
		Sequence:        1,
		Service:         "cups.service",
		State:           model.StateUp,
		EventTimeUnixMS: 1787436204558,
	})

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	r.Handler(w, req)
	body := w.Body.String()

	for _, metric := range []string{
		"systemd_service_state",
		"systemd_service_transitions_total",
		"systemd_service_last_transition_timestamp_seconds",
	} {
		if strings.Contains(body, metric) {
			t.Fatalf("/metrics unexpectedly contains %q:\n%s", metric, body)
		}
	}
	if !strings.Contains(body, "systemd_transition_exporter_dbus_connected") {
		t.Fatalf("/metrics does not contain exporter health metrics:\n%s", body)
	}
}

func TestHandlerGroupsHelpAndTypeByMetricFamily(t *testing.T) {
	r := New()
	r.SetDBusConnected(true, time.UnixMilli(1000))

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	r.Handler(w, req)
	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")

	seenHelp := map[string]bool{}
	seenType := map[string]bool{}
	for _, line := range lines {
		if strings.HasPrefix(line, "# HELP ") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				if seenHelp[parts[2]] {
					t.Fatalf("duplicate HELP for metric %q", parts[2])
				}
				seenHelp[parts[2]] = true
			}
		}
		if strings.HasPrefix(line, "# TYPE ") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				if seenType[parts[2]] {
					t.Fatalf("duplicate TYPE for metric %q", parts[2])
				}
				seenType[parts[2]] = true
			}
		}
	}
}

func TestRecoveryMetricsExposeOnlyExcludedPartBeforeSlot(t *testing.T) {
	r := New()
	loc := time.FixedZone("TEST", 3*60*60)
	observedFrom := time.Date(2026, 8, 24, 14, 47, 0, 0, loc)
	slotStart := time.Date(2026, 8, 24, 15, 0, 0, 0, loc)
	slotEnd := time.Date(2026, 8, 24, 15, 14, 0, 0, loc)
	r.RecordRecovery(observedFrom, slotStart, slotEnd)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	r.Handler(w, req)
	body := w.Body.String()
	if !strings.Contains(body, "systemd_transition_exporter_recovery_uncovered_seconds 780") {
		t.Fatalf("unexpected uncovered recovery metric:\n%s", body)
	}
}

func TestRemoteWriteMetricsAreSeparatedByTarget(t *testing.T) {
	r := New()
	r.SetRemoteWriteStats("aaaa1111", remote_write.Stats{SuccessfulRequests: 3})
	r.SetRemoteWriteStats("bbbb2222", remote_write.Stats{SuccessfulRequests: 7})
	r.AddDroppedEvents("bbbb2222", 2)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	r.Handler(w, req)
	body := w.Body.String()
	for _, want := range []string{
		`systemd_transition_exporter_remote_write_successful_requests_total{target="aaaa1111"} 3`,
		`systemd_transition_exporter_remote_write_successful_requests_total{target="bbbb2222"} 7`,
		`systemd_transition_exporter_remote_write_dropped_events_total{target="bbbb2222"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics does not contain %q:\n%s", want, body)
		}
	}
}
