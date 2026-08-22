package remote_write

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type Config struct {
	Enabled       bool
	URL           string
	BatchSize     int
	FlushInterval time.Duration
	RetryInterval time.Duration
	Timeout       time.Duration
	Checkpoint    string
}

type Sender struct {
	cfg Config
	client *http.Client
	mu sync.Mutex
	lastSent uint64
}

type checkpoint struct { LastSequence uint64 `json:"last_sequence"` }

func New(cfg Config) (*Sender, error) {
	if !cfg.Enabled { return nil, nil }
	if cfg.URL == "" { return nil, fmt.Errorf("remote_write.url is required when enabled") }
	if cfg.BatchSize <= 0 { cfg.BatchSize = 100 }
	if cfg.FlushInterval <= 0 { cfg.FlushInterval = time.Second }
	if cfg.RetryInterval <= 0 { cfg.RetryInterval = time.Second }
	if cfg.Timeout <= 0 { cfg.Timeout = 10 * time.Second }
	if cfg.Checkpoint == "" { cfg.Checkpoint = "/var/lib/systemd-transition-exporter/remote_write.checkpoint" }
	s := &Sender{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout}}
	if err := s.loadCheckpoint(); err != nil { return nil, err }
	return s, nil
}

func (s *Sender) LastSent() uint64 { s.mu.Lock(); defer s.mu.Unlock(); return s.lastSent }

// Send transmits events in sequence order. A checkpoint is committed only
// after Prometheus acknowledges the complete batch. A crash between send and
// checkpoint can therefore produce an exact duplicate, which is preferable to
// losing a transition.
func (s *Sender) Send(ctx context.Context, events []model.Event) error {
	if s == nil || len(events) == 0 { return nil }
	for start := 0; start < len(events); {
		end := start + s.cfg.BatchSize
		if end > len(events) { end = len(events) }
		batch := events[start:end]
		if err := s.sendBatch(ctx, batch); err != nil { return err }
		if err := s.saveCheckpoint(batch[len(batch)-1].Sequence); err != nil { return err }
		start = end
	}
	return nil
}

func (s *Sender) sendBatch(ctx context.Context, events []model.Event) error {
	series := make(map[string]*prompb.TimeSeries)
	for _, e := range events {
		ts := series[e.Service]
		if ts == nil {
			ts = &prompb.TimeSeries{Labels: []prompb.Label{{Name: "__name__", Value: "systemd_service_state"}, {Name: "service", Value: e.Service}}}
			series[e.Service] = ts
		}
		value := float64(0)
		if e.State == model.StateUp { value = 1 }
		ts.Samples = append(ts.Samples, prompb.Sample{Value: value, Timestamp: e.EventTimeUnixMS})
	}

	request := &prompb.WriteRequest{}
	for _, ts := range series { request.Timeseries = append(request.Timeseries, *ts) }
	payload, err := request.Marshal()
	if err != nil { return fmt.Errorf("marshal remote_write request: %w", err) }
	payload = snappy.Encode(nil, payload)

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL, bytes.NewReader(payload))
		if err != nil { return err }
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		resp, err := s.client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 { return nil }
			if resp.StatusCode >= 400 && resp.StatusCode < 500 { return fmt.Errorf("remote_write rejected request: HTTP %d", resp.StatusCode) }
		}
		timer := time.NewTimer(s.cfg.RetryInterval)
		select {
		case <-ctx.Done(): timer.Stop(); return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Sender) loadCheckpoint() error {
	data, err := os.ReadFile(s.cfg.Checkpoint)
	if os.IsNotExist(err) { return nil }
	if err != nil { return fmt.Errorf("read remote_write checkpoint: %w", err) }
	var c checkpoint
	if err := json.Unmarshal(data, &c); err != nil { return fmt.Errorf("decode remote_write checkpoint: %w", err) }
	s.lastSent = c.LastSequence
	return nil
}

func (s *Sender) saveCheckpoint(seq uint64) error {
	s.mu.Lock(); defer s.mu.Unlock()
	if seq <= s.lastSent { return nil }
	data, err := json.Marshal(checkpoint{LastSequence: seq})
	if err != nil { return err }
	if err := os.MkdirAll(filepath.Dir(s.cfg.Checkpoint), 0750); err != nil { return err }
	tmp := s.cfg.Checkpoint + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0640); err != nil { return err }
	if err := os.Rename(tmp, s.cfg.Checkpoint); err != nil { return err }
	s.lastSent = seq
	return nil
}
