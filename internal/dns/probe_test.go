package dns

import (
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestAnsweredRequiresMatchingResponse(t *testing.T) {
	id, query, err := Query()
	if err != nil {
		t.Fatal(err)
	}
	if !bytesContain(query, []byte{7, 'v', 'e', 'r', 's', 'i', 'o', 'n', 4, 'b', 'i', 'n', 'd', 0}) {
		t.Fatal("query is not CHAOS TXT version.bind")
	}
	response := append([]byte(nil), query...)
	response[2] = 0x80
	response[3] = 5 // REFUSED
	if !Answered(response, id) {
		t.Fatal("REFUSED response was rejected")
	}
	response[3] = 0
	if Answered(response, id+1) {
		t.Fatal("response for another id was accepted")
	}
	response[2] = 0
	if Answered(response, id) {
		t.Fatal("a query was accepted as a response")
	}
}

func TestProbeStopsAtFirstResponse(t *testing.T) {
	addr, requests := serveDNS(t, func(n int, query []byte) []byte {
		if n == 1 {
			return nil
		}
		reply := append([]byte(nil), query[:12]...)
		reply[2] |= 0x80
		return reply
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan time.Time, 1)
	prober := New(ctx, []Probe{{
		Service:  "named.service",
		URI:      "dns:" + addr,
		Addr:     addr,
		Interval: 30 * time.Millisecond,
	}}, func(string, time.Time) { ready <- time.Now() })
	prober.Start("named.service")
	prober.Start("named.service")

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not finish on the first DNS response")
	}
	seen := requests.Load()
	if seen < 2 {
		t.Fatalf("requests = %d, want a dropped query followed by a response", seen)
	}
	time.Sleep(100 * time.Millisecond)
	if requests.Load() != seen {
		t.Fatalf("probe kept sending after the response: %d then %d", seen, requests.Load())
	}
}

func serveDNS(t *testing.T, respond func(n int, query []byte) []byte) (string, *atomic.Int32) {
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

func bytesContain(packet, needle []byte) bool {
	return len(packet) >= len(needle) && string(packet[12:12+len(needle)]) == string(needle) &&
		binary.BigEndian.Uint16(packet[12+len(needle):12+len(needle)+2]) == typeTXT &&
		binary.BigEndian.Uint16(packet[12+len(needle)+2:12+len(needle)+4]) == classCHAOS
}
