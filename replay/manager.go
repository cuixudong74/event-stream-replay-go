package replay

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Manager coordinates multiple downstreams, each with independent live
// progress and independent replay tasks over one shared event stream. A slow
// downstream never blocks the others nor the store itself: every downstream
// has its own live goroutine and its own replay tasks, and progress is
// tracked per downstream.
type Manager struct {
	store *Store
	cps   *CheckpointStore

	mu      sync.Mutex
	sinks   map[string]Sink
	replays map[string]*Replayer // last replay task per downstream, for progress queries
}

// NewManager creates a Manager over the given store and checkpoint store.
func NewManager(store *Store, cps *CheckpointStore) *Manager {
	return &Manager{
		store:   store,
		cps:     cps,
		sinks:   make(map[string]Sink),
		replays: make(map[string]*Replayer),
	}
}

// RegisterDownstream adds a named downstream with its sink.
func (m *Manager) RegisterDownstream(name string, sink Sink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sinks[name] = sink
}

func (m *Manager) sinkOf(name string) (Sink, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sinks[name]
	if !ok {
		return nil, fmt.Errorf("manager: unknown downstream %q", name)
	}
	return s, nil
}

// StartLive starts the live processor for a downstream in the background.
// The processor stops when ctx is canceled.
func (m *Manager) StartLive(ctx context.Context, name string) error {
	sink, err := m.sinkOf(name)
	if err != nil {
		return err
	}
	lp := NewLiveProcessor(m.store, sink, m.cps, name, 10*time.Millisecond)
	go lp.Run(ctx)
	return nil
}

// StartReplay starts a replay for a downstream in the background. The
// returned channel receives the terminal error (nil on success, ctx error on
// cancellation). Replays of different downstreams proceed independently; a
// new replay for the same downstream resumes any interrupted previous run of
// the same range.
func (m *Manager) StartReplay(ctx context.Context, name string, cfg ReplayConfig) (<-chan error, error) {
	sink, err := m.sinkOf(name)
	if err != nil {
		return nil, err
	}
	r := NewReplayer(m.store, sink, m.cps, name, cfg)
	m.mu.Lock()
	m.replays[name] = r
	m.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return done, nil
}

// Progress returns the durable progress of a downstream: the committed live
// position and, when present, the replay task state. Progress of each
// downstream is independent and queryable at any time.
func (m *Manager) Progress(name string) (Checkpoint, error) {
	if _, err := m.sinkOf(name); err != nil {
		return Checkpoint{}, err
	}
	return m.cps.Load(name)
}
