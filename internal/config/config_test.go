package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func loadTestConfig(t *testing.T, remoteWrite string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := fmt.Sprintf("services:\n  - cups.service\nremote_write:\n%s\n", remoteWrite)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func TestRecoveryWindowDefaultsWhenRemoteWriteIsDisabled(t *testing.T) {
	cfg, err := loadTestConfig(t, "  enabled: false")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RemoteWrite.RecoveryWindow != 15*time.Minute {
		t.Fatalf("recovery window = %s, want 15m", cfg.RemoteWrite.RecoveryWindow)
	}
}

func TestRemoteWriteLegacyURLKeepsCheckpoint(t *testing.T) {
	cfg, err := loadTestConfig(t, "  enabled: true\n  url: http://prometheus:9090/api/v1/write\n  checkpoint: /tmp/rw.checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.RemoteWrite.Targets) != 1 {
		t.Fatalf("targets = %d, want 1", len(cfg.RemoteWrite.Targets))
	}
	target := cfg.RemoteWrite.Targets[0]
	if target.Checkpoint != "/tmp/rw.checkpoint" {
		t.Fatalf("checkpoint = %q", target.Checkpoint)
	}
	if target.LegacyCheckpoint != "" {
		t.Fatalf("legacy checkpoint = %q, want empty", target.LegacyCheckpoint)
	}
}

func TestRemoteWriteURLsHaveStableCheckpoints(t *testing.T) {
	first := "http://prometheus-a:9090/api/v1/write"
	second := "http://prometheus-b:9090/api/v1/write"
	cfg, err := loadTestConfig(t, fmt.Sprintf("  enabled: true\n  urls:\n    - %s\n    - %s\n  checkpoint: /tmp/rw.checkpoint", first, second))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.RemoteWrite.Targets) != 2 {
		t.Fatalf("targets = %d, want 2", len(cfg.RemoteWrite.Targets))
	}
	paths := map[string]string{}
	for _, target := range cfg.RemoteWrite.Targets {
		paths[target.URL] = target.Checkpoint
	}

	reordered, err := loadTestConfig(t, fmt.Sprintf("  enabled: true\n  urls:\n    - %s\n    - %s\n  checkpoint: /tmp/rw.checkpoint", second, first))
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range reordered.RemoteWrite.Targets {
		if got := paths[target.URL]; got != target.Checkpoint {
			t.Fatalf("checkpoint for %s changed from %q to %q", target.URL, got, target.Checkpoint)
		}
	}
	if cfg.RemoteWrite.Targets[0].LegacyCheckpoint != "/tmp/rw.checkpoint" {
		t.Fatalf("first target migration source = %q", cfg.RemoteWrite.Targets[0].LegacyCheckpoint)
	}
}

func TestRemoteWriteRejectsDuplicateURLs(t *testing.T) {
	_, err := loadTestConfig(t, "  enabled: true\n  urls:\n    - http://prometheus/api/v1/write\n    - http://prometheus/api/v1/write")
	if err == nil {
		t.Fatal("duplicate URLs unexpectedly accepted")
	}
}

func TestRemoteWriteRejectsMixedURLForms(t *testing.T) {
	_, err := loadTestConfig(t, "  enabled: true\n  url: http://prometheus-a/api/v1/write\n  urls:\n    - http://prometheus-b/api/v1/write")
	if err == nil {
		t.Fatal("url and urls unexpectedly accepted together")
	}
}

func TestWithListenPortKeepsHost(t *testing.T) {
	got, err := WithListenPort("0.0.0.0:9877", 9910)
	if err != nil {
		t.Fatal(err)
	}
	if got != "0.0.0.0:9910" {
		t.Fatalf("listen = %q, want 0.0.0.0:9910", got)
	}
	got, err = WithListenPort("[::1]:9877", 9100)
	if err != nil {
		t.Fatal(err)
	}
	if got != "[::1]:9100" {
		t.Fatalf("listen = %q, want [::1]:9100", got)
	}
}

func TestServicesAcceptPlainUnitsAndSIPProbes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("services:\n  - pcscf.service\n  - name: scscf.service\n    sip: sip:10.12.12.10:5060\n    interval: 700ms\n  - name: named.service\n    dns: dns:10.10.12.12:53\n    interval: 200ms\n  - name: agw.service\n    agw: agw:10.10.12.12:7722\n    interval: 500ms\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Units(); len(got) != 4 || got[0] != "pcscf.service" || got[2] != "named.service" {
		t.Fatalf("units = %v", got)
	}
	scscf := cfg.Services[1]
	if scscf.SIP != "sip:10.12.12.10:5060" || scscf.Addr != "10.12.12.10:5060" || scscf.Interval != 700*time.Millisecond {
		t.Fatalf("scscf probe = %+v", scscf)
	}
	named := cfg.Services[2]
	if named.DNS != "dns:10.10.12.12:53" || named.Addr != "10.10.12.12:53" || named.Interval != 200*time.Millisecond {
		t.Fatalf("named probe = %+v", named)
	}
	gateway := cfg.Services[3]
	if gateway.AGW != "agw:10.10.12.12:7722" || gateway.Addr != "10.10.12.12:7722" || gateway.Interval != 500*time.Millisecond {
		t.Fatalf("agw probe = %+v", gateway)
	}
	if cfg.Services[0].SIP != "" {
		t.Fatalf("plain unit gained a probe: %+v", cfg.Services[0])
	}
}

func TestServicesRejectIncompleteProbe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, body := range []string{
		"services:\n  - name: scscf.service\n    sip: sip:10.12.12.10:5060\n",
		"services:\n  - name: scscf.service\n    interval: 700ms\n",
		"services:\n  - scscf.service sip:10.12.12.10:5060 700\n",
		"services:\n  - name: scscf.service\n    sip: sip:10.12.12.10:5060;transport=tcp\n    interval: 700ms\n",
		"services:\n  - name: named.service\n    dns: dns:10.10.12.12\n    interval: 200ms\n",
		"services:\n  - name: named.service\n    sip: sip:10.12.12.10:5060\n    dns: dns:10.10.12.12:53\n    interval: 200ms\n",
		"services:\n  - name: pcscf.service\n    agw: agw:10.10.12.12:7722\n    sip: sip:10.12.12.10:5060\n    interval: 200ms\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatalf("config accepted:\n%s", body)
		}
	}
}

func TestWithListenPortRejectsOutOfRange(t *testing.T) {
	if _, err := WithListenPort("127.0.0.1:9877", 0); err == nil {
		t.Fatal("port 0 unexpectedly accepted")
	}
	if _, err := WithListenPort("127.0.0.1:9877", 65536); err == nil {
		t.Fatal("port 65536 unexpectedly accepted")
	}
}
