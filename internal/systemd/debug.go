package systemd

import "sync"

var debugDBus struct {
	mu   sync.Mutex
	conn *DBus
}

// SetDebugDBus registers the currently active D-Bus connection for the
// localhost-only debug disconnect endpoint. It is intentionally separate from
// normal monitoring logic and should only be called by the process lifecycle.
func SetDebugDBus(conn *DBus) {
	debugDBus.mu.Lock()
	defer debugDBus.mu.Unlock()
	debugDBus.conn = conn
}

// DebugDisconnect closes the collector's current D-Bus connection. The normal
// resilient monitor will observe the closed connection and reconnect according
// to its configured policy. It does not stop or restart the system D-Bus.
func DebugDisconnect() bool {
	debugDBus.mu.Lock()
	conn := debugDBus.conn
	debugDBus.mu.Unlock()
	if conn == nil {
		return false
	}
	_ = conn.Close()
	return true
}
