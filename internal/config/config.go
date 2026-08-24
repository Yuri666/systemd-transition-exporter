package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Services    []string          `yaml:"services"`
	Systemd     SystemdConfig     `yaml:"systemd"`
	WAL         WALConfig         `yaml:"wal"`
	RemoteWrite RemoteWriteConfig `yaml:"remote_write"`
}

type ServerConfig struct {
	Listen string `yaml:"listen"`
	Debug  bool   `yaml:"debug"`
}
type SystemdConfig struct {
	ReconnectInterval       time.Duration `yaml:"reconnect_interval"`
	ReconciliationInterval  time.Duration `yaml:"reconciliation_interval"`
	StartupRecoveryInterval time.Duration `yaml:"startup_recovery_interval"`
}
type WALConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Directory string `yaml:"directory"`
	Fsync     bool   `yaml:"fsync"`
}
type RemoteWriteConfig struct {
	Enabled              bool              `yaml:"enabled"`
	URL                  string            `yaml:"url"`
	BatchSize            int               `yaml:"batch_size"`
	FlushInterval        time.Duration     `yaml:"flush_interval"`
	RetryInterval        time.Duration     `yaml:"retry_interval"`
	Timeout              time.Duration     `yaml:"timeout"`
	Checkpoint           string            `yaml:"checkpoint"`
	StateInterval        time.Duration     `yaml:"state_interval"`
	RecoveryFillInterval time.Duration     `yaml:"recovery_fill_interval"`
	RecoveryWindow       time.Duration     `yaml:"recovery_window"`
	Labels               map[string]string `yaml:"labels"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	if c.Server.Listen == "" {
		c.Server.Listen = "127.0.0.1:9877"
	}
	if c.Systemd.ReconnectInterval <= 0 {
		c.Systemd.ReconnectInterval = time.Second
	}
	if c.Systemd.ReconciliationInterval <= 0 {
		c.Systemd.ReconciliationInterval = 30 * time.Second
	}
	if c.Systemd.StartupRecoveryInterval <= 0 {
		c.Systemd.StartupRecoveryInterval = 24 * time.Hour
	}
	if len(c.Services) == 0 {
		return Config{}, fmt.Errorf("services must contain at least one systemd unit")
	}
	if c.RemoteWrite.Enabled {
		if c.RemoteWrite.URL == "" {
			return Config{}, fmt.Errorf("remote_write.url must be set when remote_write.enabled=true")
		}
		if c.RemoteWrite.BatchSize <= 0 {
			c.RemoteWrite.BatchSize = 100
		}
		if c.RemoteWrite.FlushInterval <= 0 {
			c.RemoteWrite.FlushInterval = time.Second
		}
		if c.RemoteWrite.RetryInterval <= 0 {
			c.RemoteWrite.RetryInterval = time.Second
		}
		if c.RemoteWrite.Timeout <= 0 {
			c.RemoteWrite.Timeout = 10 * time.Second
		}
		if c.RemoteWrite.Checkpoint == "" {
			c.RemoteWrite.Checkpoint = "/var/lib/systemd-transition-exporter/remote_write.checkpoint"
		}
		if c.RemoteWrite.StateInterval <= 0 {
			c.RemoteWrite.StateInterval = time.Minute
		}
		if c.RemoteWrite.RecoveryFillInterval <= 0 {
			c.RemoteWrite.RecoveryFillInterval = time.Minute
		}
		if c.RemoteWrite.RecoveryWindow <= 0 {
			c.RemoteWrite.RecoveryWindow = 15 * time.Minute
		}
		// Prometheus drops a series from instant queries after its lookback
		// delta, which defaults to 5 minutes. A recovered slot must therefore
		// be republished more densely than that.
		if c.RemoteWrite.RecoveryFillInterval < 10*time.Second || c.RemoteWrite.RecoveryFillInterval > 4*time.Minute {
			return Config{}, fmt.Errorf("remote_write.recovery_fill_interval must be between 10s and 4m")
		}
		if c.RemoteWrite.RecoveryWindow < 5*time.Minute || c.RemoteWrite.RecoveryWindow > 60*time.Minute {
			return Config{}, fmt.Errorf("remote_write.recovery_window must be between 5m and 60m")
		}
		if time.Hour%c.RemoteWrite.RecoveryWindow != 0 {
			return Config{}, fmt.Errorf("remote_write.recovery_window must divide one hour exactly")
		}
		for name := range c.RemoteWrite.Labels {
			if !validLabelName(name) || name == "__name__" {
				return Config{}, fmt.Errorf("invalid remote_write label name %q", name)
			}
		}
	}
	return c, nil
}

func validLabelName(s string) bool {
	for i, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return len(s) > 0
}
