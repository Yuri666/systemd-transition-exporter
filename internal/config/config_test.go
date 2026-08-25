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
