package sip

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestIsOKMatchesCallID(t *testing.T) {
	ok := []byte("SIP/2.0 200 OK\r\nCall-ID: abc\r\nCSeq: 1 OPTIONS\r\n\r\n")
	if !IsOK(ok, []byte("abc")) {
		t.Fatal("matching 200 was rejected")
	}
	if IsOK(ok, []byte("other")) {
		t.Fatal("200 for another call was accepted")
	}
	trying := []byte("SIP/2.0 100 Trying\r\nCall-ID: abc\r\n\r\n")
	if IsOK(trying, []byte("abc")) {
		t.Fatal("provisional response was accepted as ready")
	}
}

func TestProbeStopsAtFirst200(t *testing.T) {
	addr, requests := serveSIP(t, func(n int, callID string) []byte {
		if n == 1 {
			return sipResponse(100, callID)
		}
		return sipResponse(200, callID)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prober := New(ctx, []Probe{{
		Service:  "scscf.service",
		URI:      "sip:" + addr,
		Addr:     addr,
		Interval: 30 * time.Millisecond,
	}}, nil)
	ready := make(chan time.Time, 1)
	prober.ready = func(_ string, at time.Time) { ready <- at }
	prober.Start("scscf.service")
	prober.Start("scscf.service")

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not finish on the first 200")
	}
	seen := requests.Load()
	if seen < 2 {
		t.Fatalf("requests = %d, want the non-200 followed by the 200", seen)
	}
	time.Sleep(100 * time.Millisecond)
	if requests.Load() != seen {
		t.Fatalf("probe kept sending after the 200: %d then %d", seen, requests.Load())
	}
}

func TestProbeStopBeforeAnswer(t *testing.T) {
	addr, requests := serveSIP(t, func(int, string) []byte { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prober := New(ctx, []Probe{{
		Service:  "scscf.service",
		URI:      "sip:" + addr,
		Addr:     addr,
		Interval: time.Second,
	}}, func(string, time.Time) { t.Error("ready reported after stop") })
	prober.Start("scscf.service")
	time.Sleep(20 * time.Millisecond)
	prober.Stop("scscf.service")
	time.Sleep(50 * time.Millisecond)
	stopped := requests.Load()
	time.Sleep(50 * time.Millisecond)
	if requests.Load() != stopped {
		t.Fatalf("probe sent %d more requests after stop", requests.Load()-stopped)
	}
}

func serveSIP(t *testing.T, respond func(n int, callID string) []byte) (string, *atomic.Int32) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	var requests atomic.Int32
	go func() {
		buf := make([]byte, 65535)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			seen := int(requests.Add(1))
			callID := headerValue(buf[:n], "Call-ID")
			packet := respond(seen, callID)
			if packet == nil {
				continue
			}
			_, _ = conn.WriteToUDP(packet, from)
		}
	}()
	return conn.LocalAddr().String(), &requests
}

func sipResponse(code int, callID string) []byte {
	status := map[int]string{100: "Trying", 200: "OK"}[code]
	return []byte("SIP/2.0 " + itoa(code) + " " + status + "\r\nCall-ID: " + callID + "\r\nContent-Length: 0\r\n\r\n")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [4]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func headerValue(packet []byte, name string) string {
	for _, line := range bytes.Split(packet, []byte("\n")) {
		key, value, ok := bytes.Cut(bytes.TrimSpace(line), []byte(":"))
		if ok && bytes.EqualFold(key, []byte(name)) {
			return string(bytes.TrimSpace(value))
		}
	}
	return ""
}
