// Package replay provides event-stream replay and reprocessing capabilities.
package replay

// Mode identifies which channel delivered an event to a downstream.
type Mode int

const (
	// ModeLive is delivery from the real-time tailer.
	ModeLive Mode = iota
	// ModeReplay is delivery from a historical replay.
	ModeReplay
)

// String returns a human-readable name of the delivery mode.
func (m Mode) String() string {
	switch m {
	case ModeLive:
		return "live"
	case ModeReplay:
		return "replay"
	default:
		return "unknown"
	}
}

// Event is a single record in the append-only stream.
type Event struct {
	Offset  uint64 `json:"offset"`
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
}
