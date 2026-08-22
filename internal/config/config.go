package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server  ServerConfig   `yaml:"server"`
	Services []string      `yaml:"services"`
	Systemd SystemdConfig  `yaml:"systemd"`
	WAL     WALConfig      `yaml:"wal"`
}

type ServerConfig struct {
	Listen string `yaml:"listen"`
}

type SystemdConfig struct {
	ReconnectInterval     time.Duration `yaml:"reconnect_interval"`
	ReconciliationInterval time.Duration `yaml:"reconciliation_interval"`
}

type WALConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Directory string `yaml:"directory"`
	Fsync     bool   `yaml:"fsync"`
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
	if len(c.Services) == 0 {
		return Config{}, fmt.Errorf("services must contain at least one systemd unit")
	}
	return c, nil
}
