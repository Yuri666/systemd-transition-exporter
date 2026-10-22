package dns

import (
	"crypto/rand"
	"encoding/binary"
)

// version.bind is answered by named itself. An address query could be
// forwarded or could wait on recursion, so it would measure the network
// beyond this host. CHAOS TXT does not depend on which zones are loaded, and
// a server with chaos queries disabled still answers REFUSED.
const (
	typeTXT    = 16
	classCHAOS = 3
)

// Query builds one CHAOS TXT version.bind question and returns its identifier.
func Query() (uint16, []byte, error) {
	var raw [2]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, nil, err
	}
	id := binary.BigEndian.Uint16(raw[:])
	name := []byte{7, 'v', 'e', 'r', 's', 'i', 'o', 'n', 4, 'b', 'i', 'n', 'd', 0}
	packet := make([]byte, 12+len(name)+4)
	binary.BigEndian.PutUint16(packet[0:2], id)
	binary.BigEndian.PutUint16(packet[4:6], 1)
	copy(packet[12:], name)
	off := 12 + len(name)
	binary.BigEndian.PutUint16(packet[off:off+2], typeTXT)
	binary.BigEndian.PutUint16(packet[off+2:off+4], classCHAOS)
	return id, packet, nil
}

// Answered reports whether packet is a DNS response for id. The response code
// is not checked: REFUSED and NOTIMP still mean named processed the query.
func Answered(packet []byte, id uint16) bool {
	if len(packet) < 12 {
		return false
	}
	if binary.BigEndian.Uint16(packet[0:2]) != id {
		return false
	}
	return packet[2]&0x80 != 0
}
