package sip

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// Message is one OPTIONS request and the Call-ID that identifies its response.
type Message struct {
	CallID []byte
	Bytes  []byte
}

// MaxForwards is the OPTIONS hop count for a unit. A pcscf unit gets 0 because
// that server answers only a request it must not forward, and the answer is
// 483. i-cscf and s-cscf get 70 and answer 200.
func MaxForwards(service string) int {
	if strings.Contains(strings.ToLower(service), "pcscf") {
		return 0
	}
	return 70
}

// Options builds a UDP OPTIONS request. localHost and localPort are the socket
// the request is sent from, so the response can be addressed back to it.
func Options(uri, localHost string, localPort, cseq, maxForwards int) (Message, error) {
	callID, err := token()
	if err != nil {
		return Message{}, err
	}
	branch, err := token()
	if err != nil {
		return Message{}, err
	}
	tag, err := token()
	if err != nil {
		return Message{}, err
	}
	host := localHost
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	var body strings.Builder
	fmt.Fprintf(&body, "OPTIONS %s SIP/2.0\r\n", uri)
	fmt.Fprintf(&body, "Via: SIP/2.0/UDP %s:%d;branch=z9hG4bK%s;rport\r\n", host, localPort, branch)
	fmt.Fprintf(&body, "From: <sip:systemd-transition-exporter@%s>;tag=%s\r\n", host, tag)
	fmt.Fprintf(&body, "To: <%s>\r\n", uri)
	fmt.Fprintf(&body, "Call-ID: %s\r\n", callID)
	fmt.Fprintf(&body, "CSeq: %d OPTIONS\r\n", cseq)
	fmt.Fprintf(&body, "Max-Forwards: %d\r\n", maxForwards)
	body.WriteString("Content-Length: 0\r\n\r\n")
	return Message{CallID: []byte(callID), Bytes: []byte(body.String())}, nil
}

// IsOK reports whether packet is a successful probe answer for callID.
// 200 is the keepalive reply. 483 counts only for a zero-hop request: that is
// how pcscf says it handled the OPTIONS instead of forwarding it.
func IsOK(packet, callID []byte, maxForwards int) bool {
	code, ok := statusCode(packet, callID)
	if !ok {
		return false
	}
	if code == 200 {
		return true
	}
	return code == 483 && maxForwards == 0
}

// statusCode returns the SIP status and whether callID matches. A provisional
// response is returned with ok true so the caller can keep reading.
func statusCode(packet, callID []byte) (int, bool) {
	line, _, _ := bytes.Cut(packet, []byte("\n"))
	line = bytes.TrimSpace(line)
	fields := bytes.Fields(line)
	if len(fields) < 2 || !bytes.EqualFold(fields[0], []byte("SIP/2.0")) {
		return 0, false
	}
	code, err := strconv.Atoi(string(fields[1]))
	if err != nil {
		return 0, false
	}
	for _, raw := range bytes.Split(packet, []byte("\n")) {
		raw = bytes.TrimSpace(raw)
		name, value, found := bytes.Cut(raw, []byte(":"))
		if !found {
			continue
		}
		if bytes.EqualFold(bytes.TrimSpace(name), []byte("Call-ID")) || bytes.Equal(name, []byte("i")) {
			if bytes.Equal(bytes.TrimSpace(value), callID) {
				return code, true
			}
			return 0, false
		}
	}
	return 0, false
}

func token() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
