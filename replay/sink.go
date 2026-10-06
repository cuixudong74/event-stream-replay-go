package replay

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"
)

// Reasons returned by Sink.Apply explaining the outcome of a delivery.
const (
	// ReasonApplied means the event was applied for the first time.
	ReasonApplied = "applied"
	// ReasonDuplicateRetry means the event was already applied via the same
	// channel (e.g. a retry or redelivery within live or within replay).
	ReasonDuplicateRetry = "duplicate-retry"
	// ReasonDuplicateReplay means the event was already applied via the other
	// channel (replay overlapping live, or vice versa).
	ReasonDuplicateReplay = "duplicate-replay"
)

// Delivery is one attempt to deliver an event to a downstream.
type Delivery struct {
	Event   Event
	Mode    Mode
	Attempt int // 1 for the first attempt, incremented on retries
}

// Result describes the outcome of a Sink.Apply call.
type Result struct {
	Applied bool
	Reason  string
}

// Sink is a downstream that consumes deliveries idempotently.
type Sink interface {
	Apply(ctx context.Context, d Delivery) (Result, error)
}

// DeliveryRecord logs one Apply call for reproducibility inspection.
type DeliveryRecord struct {
	Event  Event
	Mode   Mode
	Result Result
}

// MemorySink is an in-memory idempotent Sink. Its projection combines a
// per-event deterministic value (derived from a fixed seed, the event ID and
// its payload) with XOR, so the final projection is independent of delivery
// order and of duplicate deliveries: replaying the same interval with the
// same input and seed always reproduces the same per-event results and the
// same final projection.
type MemorySink struct {
	mu   sync.Mutex
	seed int64
	seen map[string]Mode // event ID -> channel that first applied it
	proj uint64
	log  []DeliveryRecord
}

// NewMemorySink creates an idempotent in-memory sink using seed for its
// deterministic projection.
func NewMemorySink(seed int64) *MemorySink {
	return &MemorySink{seed: seed, seen: make(map[string]Mode)}
}

// eventValue derives the deterministic per-event projection contribution.
func (m *MemorySink) eventValue(e Event) uint64 {
	h := fnv.New64a()
	var sb [8]byte
	binary.LittleEndian.PutUint64(sb[:], uint64(m.seed))
	h.Write(sb[:])
	h.Write([]byte(e.ID))
	h.Write([]byte{0})
	h.Write(e.Payload)
	return h.Sum64()
}

// Apply implements Sink. It is idempotent: the same event ID is applied at
// most once, and repeat deliveries are reported with a distinguishable
// reason instead of being accumulated again.
func (m *MemorySink) Apply(ctx context.Context, d Delivery) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var res Result
	prev, ok := m.seen[d.Event.ID]
	switch {
	case !ok:
		m.seen[d.Event.ID] = d.Mode
		m.proj ^= m.eventValue(d.Event)
		res = Result{Applied: true, Reason: ReasonApplied}
	case prev == d.Mode:
		res = Result{Applied: false, Reason: ReasonDuplicateRetry}
	default:
		res = Result{Applied: false, Reason: ReasonDuplicateReplay}
	}
	m.log = append(m.log, DeliveryRecord{Event: d.Event, Mode: d.Mode, Result: res})
	return res, nil
}

// Projection returns the current deterministic derived value.
func (m *MemorySink) Projection() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.proj
}

// AppliedCount returns how many distinct events were applied.
func (m *MemorySink) AppliedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.seen)
}

// Log returns the record of every Apply call in order.
func (m *MemorySink) Log() []DeliveryRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]DeliveryRecord, len(m.log))
	copy(out, m.log)
	return out
}
