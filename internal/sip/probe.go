package sip

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

// Probe is one SIP interface whose first 200 becomes the service UP time.
type Probe struct {
	Service  string
	URI      string
	Addr     string
	Interval time.Duration
}

// Prober runs at most one OPTIONS loop per service. Start during an ongoing
// probe does nothing, so periodic systemd snapshots do not reset the wait.
type Prober struct {
	ctx     context.Context
	probes  map[string]Probe
	ready   func(service string, at time.Time)
	mu      sync.Mutex
	running map[string]run
	next    uint64
}

type run struct {
	id     uint64
	cancel context.CancelFunc
}

func New(ctx context.Context, probes []Probe, ready func(service string, at time.Time)) *Prober {
	byName := make(map[string]Probe, len(probes))
	for _, probe := range probes {
		byName[probe.Service] = probe
	}
	return &Prober{
		ctx:     ctx,
		probes:  byName,
		ready:   ready,
		running: make(map[string]run),
	}
}

func (p *Prober) Start(service string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.running[service]; ok {
		return
	}
	probe, ok := p.probes[service]
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(p.ctx)
	p.next++
	id := p.next
	p.running[service] = run{id: id, cancel: cancel}
	go func() {
		defer p.finish(service, id)
		runProbe(ctx, probe, p.ready)
	}()
}

func (p *Prober) Stop(service string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	current, ok := p.running[service]
	if ok {
		delete(p.running, service)
	}
	p.mu.Unlock()
	if ok {
		current.cancel()
	}
}

func (p *Prober) finish(service string, id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, ok := p.running[service]; ok && current.id == id {
		delete(p.running, service)
	}
}

func runProbe(ctx context.Context, probe Probe, ready func(string, time.Time)) {
	log.Printf("sip probe service=%s target=%s started interval=%s", probe.Service, probe.URI, probe.Interval)
	var lastError time.Time
	report := func(err error) {
		if ctx.Err() != nil || time.Since(lastError) < time.Minute {
			return
		}
		lastError = time.Now()
		log.Printf("sip probe service=%s target=%s: %v", probe.Service, probe.URI, err)
	}
	cseq := 1
	for {
		if ctx.Err() != nil {
			log.Printf("sip probe service=%s stopped", probe.Service)
			return
		}
		answered, err := probeOnce(ctx, probe, cseq, ready)
		cseq++
		if answered || ctx.Err() != nil {
			if ctx.Err() != nil && !answered {
				log.Printf("sip probe service=%s stopped", probe.Service)
			}
			return
		}
		if err != nil {
			report(err)
		}
	}
}

// probeOnce sends one OPTIONS and waits up to the interval. A 200 invokes
// ready with the time the response arrived and reports the probe finished.
// Any other outcome returns once the interval since the send has elapsed, so
// the next request is neither sooner than the interval nor overlapping.
func probeOnce(ctx context.Context, probe Probe, cseq int, ready func(string, time.Time)) (bool, error) {
	remote, err := net.ResolveUDPAddr("udp", probe.Addr)
	if err != nil {
		wait(ctx, probe.Interval)
		return false, err
	}
	conn, err := net.DialUDP("udp", nil, remote)
	if err != nil {
		wait(ctx, probe.Interval)
		return false, err
	}
	defer conn.Close()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-stop:
		}
	}()
	local, _ := conn.LocalAddr().(*net.UDPAddr)
	if local == nil {
		wait(ctx, probe.Interval)
		return false, errors.New("local sip address is not udp")
	}
	message, err := Options(probe.URI, local.IP.String(), local.Port, cseq)
	if err != nil {
		wait(ctx, probe.Interval)
		return false, err
	}
	sent := time.Now()
	deadline := sent.Add(probe.Interval)
	_ = conn.SetWriteDeadline(deadline)
	if _, err = conn.Write(message.Bytes); err != nil {
		wait(ctx, time.Until(deadline))
		return false, err
	}
	buf := make([]byte, 65535)
	for {
		_ = conn.SetReadDeadline(deadline)
		n, err := conn.Read(buf)
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return false, nil
			}
			wait(ctx, time.Until(deadline))
			return false, err
		}
		if IsOK(buf[:n], message.CallID) {
			if ready != nil {
				ready(probe.Service, time.Now())
			}
			return true, nil
		}
		if !time.Now().Before(deadline) {
			return false, nil
		}
	}
}

func wait(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
