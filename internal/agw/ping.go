package agw

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
)

// The payload is the one pcscf sends to the local AGW. The token before the
// bencode dictionary is echoed in the answer; in the capture it is
// 0_<pid>_<counter>. The counter there steps by 24, which is the sender's own
// sequence, not a value AGW interprets. AGW answers by repeating the token
// and replacing the dictionary with d6:result4:ponge.
const (
	command = "d7:command4:pinge"
	result  = "d6:result4:ponge"
)

var seq atomic.Uint64

// Ping builds one UDP probe and returns the token a matching pong must echo.
func Ping() (token string, packet []byte) {
	n := seq.Add(1)
	token = fmt.Sprintf("0_%d_%d", os.Getpid(), n)
	return token, []byte(token + " " + command)
}

// IsPong reports whether packet is the pong for token.
func IsPong(packet []byte, token string) bool {
	prefix := []byte(token + " ")
	if !bytes.HasPrefix(packet, prefix) {
		return false
	}
	return bytes.Contains(packet[len(prefix):], []byte(result))
}
