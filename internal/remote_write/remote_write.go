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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

type Config struct {
	Enabled        bool
	URL            string
	BatchSize      int
	FlushInterval  time.Duration
	RetryInterval  time.Duration
	Timeout        time.Duration
	Checkpoint     string
	StateInterval  time.Duration
	Labels         map[string]string
}

type Stats struct {
	SuccessfulRequests uint64
	FailedRequests     uint64
	Retries            uint64
	SentSamples        uint64
}

type Sender struct {
	cfg      Config
	client   *http.Client
	mu       sync.Mutex
	sendMu   sync.Mutex
	lastSent uint64

	successfulRequests atomic.Uint64
	failedRequests     atomic.Uint64
	retries            atomic.Uint64
	sentSamples        atomic.Uint64
}

type checkpoint struct {
	LastSequence uint64 `json:"last_sequence"`
}

func New(cfg Config) (*Sender, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.URL == "" {
		return nil, fmt.Errorf("remote_write.url is required when enabled")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.StateInterval <= 0 {
		cfg.StateInterval = time.Minute
	}
	if cfg.Checkpoint == "" {
		cfg.Checkpoint = "/var/lib/systemd-transition-exporter/remote_write.checkpoint"
	}

	s := &Sender{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout}}
	if err := s.loadCheckpoint(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Sender) LastSent() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSent
}

func (s *Sender) Stats() Stats {
	return Stats{
		SuccessfulRequests: s.successfulRequests.Load(),
		FailedRequests:     s.failedRequests.Load(),
		Retries:            s.retries.Load(),
		SentSamples:        s.sentSamples.Load(),
	}
}

func (s *Sender) Send(ctx context.Context, events []model.Event) error {
	if s == nil || len(events) == 0 {
		return nil
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.sendEventsLocked(ctx, events)
}

func (s *Sender) sendEventsLocked(ctx context.Context, events []model.Event) error {
	s.mu.Lock()
	last := s.lastSent
	s.mu.Unlock()

	pending := make([]model.Event, 0, len(events))
	for _, e := range events {
		if e.Sequence > last {
			pending = append(pending, e)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	sort.SliceStable(pending, func(i, j int) bool { return pending[i].Sequence < pending[j].Sequence })

	for start := 0; start < len(pending); {
		end := start + s.cfg.BatchSize
		if end > len(pending) {
			end = len(pending)
		}
		batch := pending[start:end]
		if err := s.sendBatch(ctx, batch); err != nil {
			return err
		}
		if err := s.saveCheckpoint(batch[len(batch)-1].Sequence); err != nil {
			return fmt.Errorf("persist remote_write checkpoint after successful batch: %w", err)
		}
		start = end
	}
	return nil
}

// SendRecoveredStates sends historical state samples generated for a recovered
// interval. These samples deliberately do not advance the transition
// checkpoint because they are synthetic continuity samples rather than
// observed transitions. The caller serializes this call with transition
// delivery so samples for a series remain timestamp ordered.
func (s *Sender) SendRecoveredStates(ctx context.Context, samples []model.StateSample) error {
	if s == nil || len(samples) == 0 {
		return nil
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	series := make(map[string]*prompb.TimeSeries)
	for _, sample := range samples {
		ts := series[sample.Service]
		if ts == nil {
			ts = &prompb.TimeSeries{Labels: s.labels(sample.Service)}
			series[sample.Service] = ts
		}
		value := float64(0)
		if sample.State == model.StateUp {
			value = 1
		}
		ts.Samples = append(ts.Samples, prompb.Sample{Value: value, Timestamp: sample.TimestampUnixMS})
	}
	return s.sendSeries(ctx, series)
}

// SendCurrentStates writes heartbeat samples using the current timestamp. It
// never advances the transition checkpoint: a heartbeat is not a transition.
func (s *Sender) SendCurrentStates(ctx context.Context, states []model.ServiceState) error {
	if s == nil || len(states) == 0 {
		return nil
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	now := time.Now().UnixMilli()
	series := make(map[string]*prompb.TimeSeries)
	for _, st := range states {
		v := float64(0)
		if st.Availability == model.StateUp {
			v = 1
		}
		series[st.Service] = &prompb.TimeSeries{
			Labels:  s.labels(st.Service),
			Samples: []prompb.Sample{{Value: v, Timestamp: now}},
		}
	}
	return s.sendSeries(ctx, series)
}

func (s *Sender) labels(service string) []prompb.Label {
	labels := []prompb.Label{
		{Name: "__name__", Value: "systemd_service_state"},
		{Name: "service", Value: service},
	}
	for n, v := range s.cfg.Labels {
		if n == "__name__" || n == "service" {
			continue
		}
		labels = append(labels, prompb.Label{Name: n, Value: v})
	}
	sort.Slice(labels[2:], func(i, j int) bool {
		return labels[2+i].Name < labels[2+j].Name
	})
	return labels
}

func (s *Sender) sendBatch(ctx context.Context, events []model.Event) error {
	series := make(map[string]*prompb.TimeSeries)
	for _, e := range events {
		ts := series[e.Service]
		if ts == nil {
			ts = &prompb.TimeSeries{Labels: s.labels(e.Service)}
			series[e.Service] = ts
		}
		v := float64(0)
		if e.State == model.StateUp {
			v = 1
		}
		ts.Samples = append(ts.Samples, prompb.Sample{Value: v, Timestamp: e.EventTimeUnixMS})
	}
	return s.sendSeries(ctx, series)
}

func (s *Sender) sendSeries(ctx context.Context, series map[string]*prompb.TimeSeries) error {
	request := &prompb.WriteRequest{}
	sampleCount := 0
	for _, ts := range series {
		sort.SliceStable(ts.Samples, func(i, j int) bool {
			return ts.Samples[i].Timestamp < ts.Samples[j].Timestamp
		})
		sampleCount += len(ts.Samples)
		request.Timeseries = append(request.Timeseries, *ts)
	}
	sort.SliceStable(request.Timeseries, func(i, j int) bool {
		return request.Timeseries[i].Labels[1].Value < request.Timeseries[j].Labels[1].Value
	})

	payload, err := request.Marshal()
	if err != nil {
		return fmt.Errorf("marshal remote_write request: %w", err)
	}
	payload = snappy.Encode(nil, payload)

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

		resp, err := s.client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				s.successfulRequests.Add(1)
				s.sentSamples.Add(uint64(sampleCount))
				return nil
			}
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				s.failedRequests.Add(1)
				return fmt.Errorf("remote_write rejected request: HTTP %d", resp.StatusCode)
			}
		}

		s.failedRequests.Add(1)
		s.retries.Add(1)
		t := time.NewTimer(s.cfg.RetryInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (s *Sender) loadCheckpoint() error {
	data, err := os.ReadFile(s.cfg.Checkpoint)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read remote_write checkpoint: %w", err)
	}
	var c checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return fmt.Errorf("decode remote_write checkpoint: %w", err)
	}
	s.lastSent = c.LastSequence
	return nil
}

// saveCheckpoint makes the delivery checkpoint durable before returning. The
// sequence is advanced only after the corresponding Remote Write request has
// returned a successful 2xx response. The temporary file is synced before the
// atomic rename and the containing directory is synced afterwards.
func (s *Sender) saveCheckpoint(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq <= s.lastSent {
		return nil
	}
	data, err := json.Marshal(checkpoint{LastSequence: seq})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.cfg.Checkpoint), 0750); err != nil {
		return err
	}
	tmp := s.cfg.Checkpoint + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		return fmt.Errorf("open checkpoint temporary file: %w", err)
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()

	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write checkpoint temporary file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint temporary file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close checkpoint temporary file: %w", err)
	}
	if err := os.Rename(tmp, s.cfg.Checkpoint); err != nil {
		return fmt.Errorf("rename checkpoint temporary file: %w", err)
	}

	dir, err := os.Open(filepath.Dir(s.cfg.Checkpoint))
	if err != nil {
		return fmt.Errorf("open checkpoint directory: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("sync checkpoint directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close checkpoint directory: %w", err)
	}

	s.lastSent = seq
	ok = true
	return nil
}
