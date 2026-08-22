package model

import "time"

type AvailabilityState uint8
const (
	StateUnknown AvailabilityState = iota
	StateUp
	StateDown
)
func (s AvailabilityState) String() string { switch s { case StateUp: return "up"; case StateDown: return "down"; default: return "unknown" } }

type EventSource uint8
const (
	SourceSystemd EventSource = iota
	SourceHostReboot
	SourceRecovery
)
func (s EventSource) String() string { switch s { case SourceSystemd: return "systemd"; case SourceHostReboot: return "host_reboot"; case SourceRecovery: return "recovery"; default: return "unknown" } }

type UnitSnapshot struct {
	Service string
	ActiveState string
	SubState string
	ActiveEnterTimestampUS uint64
	ActiveExitTimestampUS uint64
	ActiveEnterTimestampMonotonicUS uint64
	ActiveExitTimestampMonotonicUS uint64
	BootID string
	ObservedAt time.Time
}

type Event struct {
	Sequence uint64
	Service string
	State AvailabilityState
	EventTimeUnixMS int64
	EventTimeMonotonicUS uint64
	BootID string
	Source EventSource
	SystemdActiveState string
	SystemdSubState string
}

type ServiceState struct {
	Service string
	Availability AvailabilityState
	ActiveState string
	SubState string
	BootID string
	LastActiveEnterTimestampUS uint64
	LastActiveExitTimestampUS uint64
	LastActiveEnterMonotonicUS uint64
	LastActiveExitMonotonicUS uint64
	LastEventTimeUnixMS int64
	LastEventMonoUS uint64
	LastSequence uint64
}
