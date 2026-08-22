package systemd

import (
	"context"
	"fmt"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

// ReconnectLoop is the lifecycle coordinator for D-Bus. It deliberately
// separates connection loss from service state: during a D-Bus outage the
// last known state is retained, and snapshots are reconciled after reconnect.
type ReconnectLoop struct {
	interval time.Duration
}

func NewReconnectLoop(interval time.Duration) *ReconnectLoop {
	if interval <= 0 { interval = time.Second }
	return &ReconnectLoop{interval: interval}
}

// Run executes connectFn until it succeeds or ctx is cancelled. connectFn is
// responsible for one complete connection lifetime and returns when the
// connection is lost. This primitive is intentionally independent of D-Bus so
// it can be unit-tested without a system bus.
func (r *ReconnectLoop) Run(ctx context.Context, connectFn func(context.Context) error) error {
	for {
		err := connectFn(ctx)
		if ctx.Err() != nil { return ctx.Err() }
		if err == nil { return nil }
		t := time.NewTimer(r.interval)
		select {
		case <-ctx.Done(): t.Stop(); return ctx.Err()
		case <-t.C:
		}
	}
}

func snapshotState(s model.UnitSnapshot) model.AvailabilityState {
	switch s.ActiveState {
	case "active", "activating": return model.StateUp
	case "inactive", "failed", "deactivating": return model.StateDown
	default: return model.StateUnknown
	}
}

var _ = fmt.Sprintf
var _ = snapshotState
