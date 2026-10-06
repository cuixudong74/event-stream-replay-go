package replay

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// defaultCommitEvery is the default number of events between checkpoint commits.
const defaultCommitEvery = 16

// ReplayConfig configures a single replay task.
type ReplayConfig struct {
	From uint64 // inclusive range start
	To   uint64 // inclusive range end

	RatePerSec  float64 // max events per second; <= 0 means unlimited
	Concurrency int     // parallel deliveries within a commit batch; <= 1 means 1
	CommitEvery int     // events applied between checkpoint commits; <= 0 means a default
}

// Replayer reprocesses a historical range for one downstream. Progress is
// committed to durable storage every CommitEvery events, so an interrupted
// run resumes from the last committed position without re-applying or
// skipping any committed event.
type Replayer struct {
	store *Store
	sink  Sink
	cps   *CheckpointStore
	name  string
	cfg   ReplayConfig

	mu    sync.Mutex
	state ReplayState
}

// NewReplayer creates a replayer for downstream reading from store, writing to
// sink and persisting progress in cps.
func NewReplayer(store *Store, sink Sink, cps *CheckpointStore, downstream string, cfg ReplayConfig) *Replayer {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.CommitEvery <= 0 {
		cfg.CommitEvery = defaultCommitEvery
	}
	return &Replayer{
		store: store,
		sink:  sink,
		cps:   cps,
		name:  downstream,
		cfg:   cfg,
		state: ReplayState{From: cfg.From, To: cfg.To, Next: cfg.From, Status: StatusRunning},
	}
}

// Run executes the replay, resuming from the last committed position if a
// previous run was interrupted or canceled. Cancellation via ctx leaves a
// resumable checkpoint (StatusCanceled) and returns the ctx error; the
// downstream is never left in a partially committed batch state.
func (r *Replayer) Run(ctx context.Context) error {
	if r.cfg.From == 0 || r.cfg.From > r.cfg.To {
		return fmt.Errorf("replayer: invalid range [%d, %d]", r.cfg.From, r.cfg.To)
	}
	if latest := r.store.Latest(); r.cfg.To > latest {
		return fmt.Errorf("replayer: range end %d beyond latest committed offset %d", r.cfg.To, latest)
	}

	cp, err := r.cps.Load(r.name)
	if err != nil {
		return err
	}
	next := r.cfg.From
	if cp.Replay != nil && cp.Replay.Status != StatusDone &&
		cp.Replay.From == r.cfg.From && cp.Replay.To == r.cfg.To {
		// Resume an interrupted or canceled run of the same range.
		next = cp.Replay.Next
	}
	if err := r.save(ReplayState{From: r.cfg.From, To: r.cfg.To, Next: next, Status: StatusRunning}); err != nil {
		return err
	}

	lim := newLimiter(r.cfg.RatePerSec)
	for next <= r.cfg.To {
		if err := ctx.Err(); err != nil {
			return r.cancel(next, err)
		}
		end := next + uint64(r.cfg.CommitEvery) - 1
		if end > r.cfg.To {
			end = r.cfg.To
		}
		events, err := r.store.Read(next, end)
		if err != nil {
			return err
		}
		if err := r.applyBatch(ctx, lim, events); err != nil {
			if ctx.Err() != nil {
				return r.cancel(next, ctx.Err())
			}
			return err
		}
		next = end + 1
		if err := r.save(ReplayState{From: r.cfg.From, To: r.cfg.To, Next: next, Status: StatusRunning}); err != nil {
			return err
		}
	}
	return r.save(ReplayState{From: r.cfg.From, To: r.cfg.To, Next: next, Status: StatusDone})
}

// applyBatch delivers one commit batch with the configured concurrency and
// waits for the whole batch before returning, so checkpoints always advance
// over a contiguous, fully applied prefix.
func (r *Replayer) applyBatch(ctx context.Context, lim *limiter, events []Event) error {
	sem := make(chan struct{}, r.cfg.Concurrency)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	dispatched := 0
	for _, e := range events {
		if ctx.Err() != nil {
			break
		}
		if err := lim.wait(ctx); err != nil {
			break
		}
		dispatched++
		wg.Add(1)
		sem <- struct{}{}
		go func(e Event) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := r.sink.Apply(ctx, Delivery{Event: e, Mode: ModeReplay, Attempt: 1}); err != nil {
				once.Do(func() { firstErr = err })
			}
		}(e)
	}
	wg.Wait()
	if dispatched < len(events) {
		// The batch was cut short (cancellation): it must not be committed,
		// otherwise undelivered events would be skipped.
		return ctx.Err()
	}
	return firstErr
}

// cancel records a resumable canceled state and returns err.
func (r *Replayer) cancel(next uint64, err error) error {
	if serr := r.save(ReplayState{From: r.cfg.From, To: r.cfg.To, Next: next, Status: StatusCanceled}); serr != nil {
		return serr
	}
	return err
}

// save atomically merges the replay state into the downstream checkpoint
// (preserving the concurrently advancing live progress) and updates the
// in-memory progress view.
func (r *Replayer) save(st ReplayState) error {
	if _, err := r.cps.Update(r.name, func(cp Checkpoint) Checkpoint {
		cp.Replay = &st
		return cp
	}); err != nil {
		return err
	}
	r.mu.Lock()
	r.state = st
	r.mu.Unlock()
	return nil
}

// Progress returns the current replay state.
func (r *Replayer) Progress() ReplayState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// limiter is a minimal interval-based rate limiter.
type limiter struct {
	interval time.Duration
	next     time.Time
}

func newLimiter(ratePerSec float64) *limiter {
	l := &limiter{}
	if ratePerSec > 0 {
		l.interval = time.Duration(float64(time.Second) / ratePerSec)
	}
	return l
}

func (l *limiter) wait(ctx context.Context) error {
	if l.interval <= 0 {
		return ctx.Err()
	}
	now := time.Now()
	if l.next.IsZero() || now.After(l.next) {
		l.next = now.Add(l.interval)
		return ctx.Err()
	}
	t := time.NewTimer(l.next.Sub(now))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		l.next = l.next.Add(l.interval)
		return nil
	}
}
