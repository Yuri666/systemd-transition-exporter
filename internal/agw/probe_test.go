package agw

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPingMatchesCapturedExchange(t *testing.T) {
	token, packet := Ping()
	text := string(packet)
	if !strings.HasPrefix(text, token+" ") || !strings.HasSuffix(text, " d7:command4:pinge") {
		t.Fatalf("ping = %q", text)
	}
	pong := []byte(token + " d6:result4:ponge")
	if !IsPong(pong, token) {
		t.Fatal("matching pong was rejected")
	}
	if IsPong([]byte("0_1_9 d6:result4:ponge"), token) {
		t.Fatal("pong for another token was accepted")
	}
	if IsPong([]byte(token+" d7:command4:pinge"), token) {
		t.Fatal("the ping itself was accepted as a pong")
	}
}

func TestProbeStopsAtFirstPong(t *testing.T) {
	addr, requests := serveAGW(t, func(n int, query []byte) []byte {
		if n == 1 {
			return nil
		}
		token, _, _ := strings.Cut(string(query), " ")
		return []byte(token + " d6:result4:ponge")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan time.Time, 1)
	prober := New(ctx, []Probe{{
		Service:  "pcscf.service",
		URI:      "agw:" + addr,
		Addr:     addr,
		Interval: 30 * time.Millisecond,
	}}, func(string, time.Time) { ready <- time.Now() })
	prober.Start("pcscf.service")
	prober.Start("pcscf.service")

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not finish on the first pong")
	}
	seen := requests.Load()
	if seen < 2 {
		t.Fatalf("requests = %d, want a dropped ping followed by a pong", seen)
	}
	time.Sleep(100 * time.Millisecond)
	if requests.Load() != seen {
		t.Fatalf("probe kept sending after the pong: %d then %d", seen, requests.Load())
	}
}

func serveAGW(t *testing.T, respond func(n int, query []byte) []byte) (string, *atomic.Int32) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	var requests atomic.Int32
	go func() {
		buf := make([]byte, 4096)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			query := append([]byte(nil), buf[:n]...)
			seen := int(requests.Add(1))
			packet := respond(seen, query)
			if packet == nil {
				continue
			}
			_, _ = conn.WriteToUDP(packet, from)
		}
	}()
	return conn.LocalAddr().String(), &requests
}
