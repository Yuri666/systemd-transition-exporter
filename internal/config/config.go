package config

import (
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Services    []Service         `yaml:"services"`
	Systemd     SystemdConfig     `yaml:"systemd"`
	WAL         WALConfig         `yaml:"wal"`
	RemoteWrite RemoteWriteConfig `yaml:"remote_write"`
}

// Service is one monitored systemd unit. A SIP target delays the UP transition
// until that interface answers OPTIONS with 200; a unit without one is tracked
// from systemd alone.
type Service struct {
	Name     string
	SIP      string
	Addr     string
	Interval time.Duration
}

func (s *Service) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		s.Name = strings.TrimSpace(value.Value)
		return nil
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("service must be a unit name or a name/sip/interval mapping")
	}
	var raw struct {
		Name     string     `yaml:"name"`
		SIP      string     `yaml:"sip"`
		Interval probeEvery `yaml:"interval"`
	}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	*s = Service{Name: strings.TrimSpace(raw.Name), SIP: strings.TrimSpace(raw.SIP), Interval: time.Duration(raw.Interval)}
	return nil
}

// probeEvery accepts a Go duration (700ms) or a bare number of milliseconds,
// which is the unit the probe interval is configured in.
type probeEvery time.Duration

func (p *probeEvery) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("interval must be a duration or a number of milliseconds")
	}
	switch value.Tag {
	case "!!int", "!!float":
		n, err := strconv.ParseFloat(value.Value, 64)
		if err != nil {
			return fmt.Errorf("interval %q: %w", value.Value, err)
		}
		*p = probeEvery(time.Duration(n * float64(time.Millisecond)))
		return nil
	default:
		d, err := time.ParseDuration(strings.TrimSpace(value.Value))
		if err != nil {
			return fmt.Errorf("interval %q: %w", value.Value, err)
		}
		*p = probeEvery(d)
		return nil
	}
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
	Enabled              bool                `yaml:"enabled"`
	URL                  string              `yaml:"url"`
	URLs                 []string            `yaml:"urls"`
	BatchSize            int                 `yaml:"batch_size"`
	FlushInterval        time.Duration       `yaml:"flush_interval"`
	RetryInterval        time.Duration       `yaml:"retry_interval"`
	Timeout              time.Duration       `yaml:"timeout"`
	Checkpoint           string              `yaml:"checkpoint"`
	StateInterval        time.Duration       `yaml:"state_interval"`
	RecoveryFillInterval time.Duration       `yaml:"recovery_fill_interval"`
	RecoveryWindow       time.Duration       `yaml:"recovery_window"`
	Labels               map[string]string   `yaml:"labels"`
	Targets              []RemoteWriteTarget `yaml:"-"`
}

type RemoteWriteTarget struct {
	ID               string
	URL              string
	Checkpoint       string
	LegacyCheckpoint string
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
	seen := make(map[string]struct{}, len(c.Services))
	for i := range c.Services {
		svc := &c.Services[i]
		if svc.Name == "" || strings.ContainsAny(svc.Name, " \t") {
			return Config{}, fmt.Errorf("services[%d] must name a systemd unit", i)
		}
		if _, ok := seen[svc.Name]; ok {
			return Config{}, fmt.Errorf("services contains duplicate unit %q", svc.Name)
		}
		seen[svc.Name] = struct{}{}
		if svc.SIP == "" {
			if svc.Interval != 0 {
				return Config{}, fmt.Errorf("services[%d] interval requires sip", i)
			}
			continue
		}
		if svc.Interval <= 0 {
			return Config{}, fmt.Errorf("services[%d] sip probe requires a positive interval", i)
		}
		uri, addr, err := parseSIPTarget(svc.SIP)
		if err != nil {
			return Config{}, fmt.Errorf("services[%d]: %w", i, err)
		}
		svc.SIP = uri
		svc.Addr = addr
	}
	// Recovery also protects D-Bus reconnects and host-reboot handling, so its
	// slot size must be valid even when Remote Write delivery is disabled.
	if c.RemoteWrite.RecoveryFillInterval <= 0 {
		c.RemoteWrite.RecoveryFillInterval = time.Minute
	}
	if c.RemoteWrite.RecoveryWindow <= 0 {
		c.RemoteWrite.RecoveryWindow = 15 * time.Minute
	}
	if c.RemoteWrite.Enabled {
		if c.RemoteWrite.URL != "" && len(c.RemoteWrite.URLs) > 0 {
			return Config{}, fmt.Errorf("remote_write.url and remote_write.urls cannot be used together")
		}
		urls := c.RemoteWrite.URLs
		legacy := false
		if len(urls) == 0 && c.RemoteWrite.URL != "" {
			urls = []string{c.RemoteWrite.URL}
			legacy = true
		}
		if len(urls) == 0 {
			return Config{}, fmt.Errorf("remote_write.url or remote_write.urls must be set when remote_write.enabled=true")
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
		seenURLs := make(map[string]struct{}, len(urls))
		for i, rawURL := range urls {
			url := strings.TrimSpace(rawURL)
			if url == "" {
				return Config{}, fmt.Errorf("remote_write.urls[%d] must not be empty", i)
			}
			if _, ok := seenURLs[url]; ok {
				return Config{}, fmt.Errorf("remote_write contains duplicate URL %q", url)
			}
			seenURLs[url] = struct{}{}
			sum := sha256.Sum256([]byte(url))
			id := fmt.Sprintf("%x", sum[:6])
			checkpoint := c.RemoteWrite.Checkpoint
			legacyCheckpoint := ""
			if !legacy {
				checkpoint += "." + id
				if i == 0 {
					legacyCheckpoint = c.RemoteWrite.Checkpoint
				}
			}
			c.RemoteWrite.Targets = append(c.RemoteWrite.Targets, RemoteWriteTarget{
				ID:               id,
				URL:              url,
				Checkpoint:       checkpoint,
				LegacyCheckpoint: legacyCheckpoint,
			})
		}
		if c.RemoteWrite.StateInterval <= 0 {
			c.RemoteWrite.StateInterval = time.Minute
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

// Units returns the systemd unit names in configuration order.
func (c Config) Units() []string {
	names := make([]string, len(c.Services))
	for i, svc := range c.Services {
		names[i] = svc.Name
	}
	return names
}

// parseSIPTarget accepts sip:host:port. UDP is the only transport, so a
// transport parameter is rejected rather than ignored.
func parseSIPTarget(raw string) (uri, addr string, err error) {
	raw = strings.TrimSpace(raw)
	const prefix = "sip:"
	if len(raw) < len(prefix) || !strings.EqualFold(raw[:len(prefix)], prefix) {
		return "", "", fmt.Errorf("sip target %q must start with sip:", raw)
	}
	rest := raw[len(prefix):]
	if strings.ContainsAny(rest, ";?") || strings.Contains(rest, "@") {
		return "", "", fmt.Errorf("sip target %q accepts host and port only", raw)
	}
	host, port, err := net.SplitHostPort(rest)
	if err != nil {
		return "", "", fmt.Errorf("sip target %q: %w", raw, err)
	}
	if host == "" {
		return "", "", fmt.Errorf("sip target %q has an empty host", raw)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", "", fmt.Errorf("sip target %q has an invalid port", raw)
	}
	addr = net.JoinHostPort(host, port)
	return "sip:" + addr, addr, nil
}

// WithListenPort replaces the TCP port in a host:port listen address. The host
// is taken from listen; an empty listen binds all addresses on the new port.
func WithListenPort(listen string, port int) (string, error) {
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("web.listen-port must be between 1 and 65535")
	}
	host := ""
	if strings.TrimSpace(listen) != "" {
		var err error
		host, _, err = net.SplitHostPort(listen)
		if err != nil {
			return "", fmt.Errorf("server.listen %q: %w", listen, err)
		}
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
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
