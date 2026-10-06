package replay

import (
	"context"
	"sync"
	"time"
)

// LiveProcessor tails the store and delivers new events to one downstream.
// It runs independently of any replay of the same downstream: events arriving
// during a replay are delivered in offset order and committed, and the
// idempotent sink resolves any overlap with the replayed range.
type LiveProcessor struct {
	store *Store
	sink  Sink
	cps   *CheckpointStore
	name  string
	poll  time.Duration

	mu        sync.Mutex
	committed uint64
}

// NewLiveProcessor creates a live processor for downstream. poll is how often
// the store is checked for new events.
func NewLiveProcessor(store *Store, sink Sink, cps *CheckpointStore, downstream string, poll time.Duration) *LiveProcessor {
	if poll <= 0 {
		poll = 10 * time.Millisecond
	}
	return &LiveProcessor{store: store, sink: sink, cps: cps, name: downstream, poll: poll}
}

// Run tails the store until ctx is canceled. New events arriving during a
// concurrent replay are never lost nor skipped. It always returns nil on
// context cancellation; only store or sink failures are reported.
func (l *LiveProcessor) Run(ctx context.Context) error {
	cp, err := l.cps.Load(l.name)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.committed = cp.LiveCommitted
	l.mu.Unlock()

	tick := time.NewTicker(l.poll)
	defer tick.Stop()
	for {
		if err := l.drain(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			// Best-effort final drain so nothing committed to the store is
			// left unprocessed when the pipeline shuts down cleanly.
			_ = l.drain(context.Background())
			return nil
		case <-tick.C:
		}
	}
}

// drain applies all events after the committed offset up to the current
// latest, then commits the new position atomically.
func (l *LiveProcessor) drain(ctx context.Context) error {
	for {
		from := l.Committed() + 1
		latest := l.store.Latest()
		if from > latest {
			return nil
		}
		events, err := l.store.Read(from, latest)
		if err != nil {
			return err
		}
		for _, e := range events {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := l.sink.Apply(ctx, Delivery{Event: e, Mode: ModeLive, Attempt: 1}); err != nil {
				return err
			}
			cp, err := l.cps.Update(l.name, func(cp Checkpoint) Checkpoint {
				if e.Offset > cp.LiveCommitted {
					cp.LiveCommitted = e.Offset
				}
				return cp
			})
			if err != nil {
				return err
			}
			l.mu.Lock()
			l.committed = cp.LiveCommitted
			l.mu.Unlock()
		}
	}
}

// Committed returns the last committed live offset.
func (l *LiveProcessor) Committed() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.committed
}
