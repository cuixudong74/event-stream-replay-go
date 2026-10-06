package replay

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const testSeed = 42

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "events.log"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func appendN(t *testing.T, s *Store, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := s.Append(Event{ID: fmt.Sprintf("e%d", s.Latest()+1), Payload: []byte(fmt.Sprintf("payload-%d", s.Latest()+1))}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --- 1. Range boundaries ---------------------------------------------------

func TestStoreRangeBoundaries(t *testing.T) {
	s := newStore(t)
	appendN(t, s, 10)

	evs, err := s.Read(3, 7)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(evs) != 5 {
		t.Fatalf("got %d events, want 5", len(evs))
	}
	for i, e := range evs {
		if want := uint64(3 + i); e.Offset != want {
			t.Fatalf("event %d has offset %d, want %d", i, e.Offset, want)
		}
	}

	one, err := s.Read(1, 1)
	if err != nil || len(one) != 1 || one[0].Offset != 1 {
		t.Fatalf("single first: %v %v", one, err)
	}
	last, err := s.Read(10, 10)
	if err != nil || len(last) != 1 || last[0].Offset != 10 {
		t.Fatalf("single last: %v %v", last, err)
	}
	for _, bad := range [][2]uint64{{0, 1}, {5, 4}, {1, 11}, {11, 11}} {
		if _, err := s.Read(bad[0], bad[1]); err == nil {
			t.Fatalf("range %v should fail", bad)
		}
	}
}

func TestReplayRangeBoundaries(t *testing.T) {
	s := newStore(t)
	appendN(t, s, 10)
	cps := NewCheckpointStore(t.TempDir())
	sink := NewMemorySink(testSeed)

	r := NewReplayer(s, sink, cps, "d", ReplayConfig{From: 3, To: 7})
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := sink.AppliedCount(); got != 5 {
		t.Fatalf("applied %d events, want exactly 5 (no more, no less)", got)
	}
	for _, rec := range sink.Log() {
		if rec.Event.Offset < 3 || rec.Event.Offset > 7 {
			t.Fatalf("event %d outside replayed range", rec.Event.Offset)
		}
	}
	if r.Progress().Status != StatusDone {
		t.Fatalf("status = %q, want done", r.Progress().Status)
	}
}

// --- 2. Replay concurrent with live ---------------------------------------

func TestReplayConcurrentWithLive(t *testing.T) {
	s := newStore(t)
	appendN(t, s, 50)
	cps := NewCheckpointStore(t.TempDir())
	sink := NewMemorySink(testSeed)

	ctx, cancel := context.WithCancel(context.Background())
	lp := NewLiveProcessor(s, sink, cps, "d", time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); lp.Run(ctx) }()

	// Replay the historical range while new events keep arriving.
	r := NewReplayer(s, sink, cps, "d", ReplayConfig{From: 1, To: 50, CommitEvery: 4})
	replayDone := make(chan error, 1)
	go func() { replayDone <- r.Run(context.Background()) }()

	appendN(t, s, 50) // live events 51..100 arriving during replay

	if err := <-replayDone; err != nil {
		t.Fatalf("replay: %v", err)
	}
	waitFor(t, "live to drain", func() bool { return lp.Committed() == 100 })
	cancel()
	wg.Wait()

	if got := sink.AppliedCount(); got != 100 {
		t.Fatalf("applied %d distinct events, want 100 (nothing lost, nothing skipped)", got)
	}

	// Reference: one full replay of the whole history into a fresh sink.
	refSink := NewMemorySink(testSeed)
	ref := NewReplayer(s, refSink, NewCheckpointStore(t.TempDir()), "ref", ReplayConfig{From: 1, To: 100})
	if err := ref.Run(context.Background()); err != nil {
		t.Fatalf("reference replay: %v", err)
	}
	if sink.Projection() != refSink.Projection() {
		t.Fatalf("projection %d != full-replay projection %d", sink.Projection(), refSink.Projection())
	}
}

// --- 3. Idempotent duplicate delivery --------------------------------------

func TestDuplicateDeliveryReasons(t *testing.T) {
	sink := NewMemorySink(testSeed)
	e := Event{Offset: 1, ID: "e1", Payload: []byte("x")}
	ctx := context.Background()

	r1, _ := sink.Apply(ctx, Delivery{Event: e, Mode: ModeLive, Attempt: 1})
	if !r1.Applied || r1.Reason != ReasonApplied {
		t.Fatalf("first delivery: %+v", r1)
	}
	p := sink.Projection()

	r2, _ := sink.Apply(ctx, Delivery{Event: e, Mode: ModeLive, Attempt: 2})
	if r2.Applied || r2.Reason != ReasonDuplicateRetry {
		t.Fatalf("same-channel retry: %+v", r2)
	}
	r3, _ := sink.Apply(ctx, Delivery{Event: e, Mode: ModeReplay, Attempt: 1})
	if r3.Applied || r3.Reason != ReasonDuplicateReplay {
		t.Fatalf("cross-channel replay: %+v", r3)
	}
	if sink.Projection() != p || sink.AppliedCount() != 1 {
		t.Fatalf("duplicates changed downstream state")
	}
}

// --- 4. Interrupt and resume -----------------------------------------------

func TestInterruptResume(t *testing.T) {
	s := newStore(t)
	appendN(t, s, 40)
	dir := t.TempDir()
	sink := NewMemorySink(testSeed)

	// First run: cancel after a few commits.
	ctx, cancel := context.WithCancel(context.Background())
	r1 := NewReplayer(s, sink, NewCheckpointStore(dir), "d",
		ReplayConfig{From: 1, To: 40, CommitEvery: 4, RatePerSec: 200})
	done := make(chan error, 1)
	go func() { done <- r1.Run(ctx) }()
	waitFor(t, "first commit", func() bool { return r1.Progress().Next > 1 })
	cancel()
	if err := <-done; err == nil {
		t.Fatalf("canceled run should return ctx error")
	}

	interrupted := r1.Progress().Next
	if interrupted <= 1 || interrupted > 40 {
		t.Fatalf("interrupted at Next=%d, expected mid-range", interrupted)
	}

	// "Restart": fresh checkpoint store over the same dir; committed position
	// and the unfinished replay must be distinguishable.
	cp, err := NewCheckpointStore(dir).Load("d")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if cp.Replay == nil || cp.Replay.Status != StatusCanceled {
		t.Fatalf("checkpoint after cancel: %+v", cp.Replay)
	}
	if cp.Replay.Next != interrupted {
		t.Fatalf("committed Next=%d, want %d", cp.Replay.Next, interrupted)
	}

	// Resume: must continue from the committed position without re-applying
	// or skipping anything.
	r2 := NewReplayer(s, sink, NewCheckpointStore(dir), "d", ReplayConfig{From: 1, To: 40, CommitEvery: 4})
	if err := r2.Run(context.Background()); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := sink.AppliedCount(); got != 40 {
		t.Fatalf("applied %d distinct events, want 40", got)
	}
	applied := 0
	for _, rec := range sink.Log() {
		if rec.Result.Reason == ReasonApplied {
			applied++
		}
	}
	if applied != 40 {
		t.Fatalf("log shows %d applied deliveries, want exactly 40", applied)
	}
	cp, _ = NewCheckpointStore(dir).Load("d")
	if cp.Replay.Status != StatusDone {
		t.Fatalf("final status = %q, want done", cp.Replay.Status)
	}
}

// --- 5. Multiple downstreams with independent progress ---------------------

func TestMultipleDownstreamsIndependent(t *testing.T) {
	s := newStore(t)
	appendN(t, s, 30)
	m := NewManager(s, NewCheckpointStore(t.TempDir()))
	slow := NewMemorySink(1)
	fast := NewMemorySink(2)
	m.RegisterDownstream("slow", slow)
	m.RegisterDownstream("fast", fast)

	ctx := context.Background()
	slowDone, err := m.StartReplay(ctx, "slow", ReplayConfig{From: 1, To: 30, RatePerSec: 300, CommitEvery: 2})
	if err != nil {
		t.Fatal(err)
	}
	fastDone, err := m.StartReplay(ctx, "fast", ReplayConfig{From: 1, To: 30})
	if err != nil {
		t.Fatal(err)
	}

	if err := <-fastDone; err != nil {
		t.Fatalf("fast replay: %v", err)
	}
	// The fast downstream finished; the slow one must still be behind and
	// its progress must be queryable independently.
	cp, err := m.Progress("slow")
	if err != nil {
		t.Fatal(err)
	}
	if cp.Replay == nil || cp.Replay.Status != StatusRunning || cp.Replay.Next > 31 {
		t.Fatalf("slow progress: %+v", cp.Replay)
	}
	if err := <-slowDone; err != nil {
		t.Fatalf("slow replay: %v", err)
	}
	if slow.AppliedCount() != 30 || fast.AppliedCount() != 30 {
		t.Fatalf("slow=%d fast=%d, want 30 each", slow.AppliedCount(), fast.AppliedCount())
	}
	for _, name := range []string{"slow", "fast"} {
		cp, _ := m.Progress(name)
		if cp.Replay.Status != StatusDone {
			t.Fatalf("%s status = %q", name, cp.Replay.Status)
		}
	}
}

// --- 6. Rate limiting, cancel and timeout ----------------------------------

func TestReplayRateLimit(t *testing.T) {
	s := newStore(t)
	appendN(t, s, 20)
	sink := NewMemorySink(testSeed)
	r := NewReplayer(s, sink, NewCheckpointStore(t.TempDir()), "d",
		ReplayConfig{From: 1, To: 20, RatePerSec: 100, CommitEvery: 20})
	start := time.Now()
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Fatalf("20 events at 100/s took %v, rate limit not enforced", d)
	}
}

func TestCancelTimeoutResumable(t *testing.T) {
	s := newStore(t)
	appendN(t, s, 50)
	dir := t.TempDir()
	sink := NewMemorySink(testSeed)

	// Run under a tight timeout; the slow rate guarantees cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r1 := NewReplayer(s, sink, NewCheckpointStore(dir), "d",
		ReplayConfig{From: 1, To: 50, RatePerSec: 50, CommitEvery: 2})
	if err := r1.Run(ctx); err == nil {
		t.Fatalf("expected timeout error")
	}
	st := r1.Progress()
	if st.Status != StatusCanceled || st.Next <= 1 || st.Next > 50 {
		t.Fatalf("after timeout: %+v", st)
	}

	// The canceled replay restarts from an explicit, committed position.
	r2 := NewReplayer(s, sink, NewCheckpointStore(dir), "d", ReplayConfig{From: 1, To: 50, CommitEvery: 2})
	if err := r2.Run(context.Background()); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := sink.AppliedCount(); got != 50 {
		t.Fatalf("applied %d, want 50", got)
	}
}

// --- 7. Reproducibility ----------------------------------------------------

func TestReplayReproducible(t *testing.T) {
	s := newStore(t)
	appendN(t, s, 30)

	run := func() (*MemorySink, []DeliveryRecord) {
		sink := NewMemorySink(testSeed)
		r := NewReplayer(s, sink, NewCheckpointStore(t.TempDir()), "d",
			ReplayConfig{From: 5, To: 25, Concurrency: 4, CommitEvery: 3})
		if err := r.Run(context.Background()); err != nil {
			t.Fatalf("replay: %v", err)
		}
		return sink, sink.Log()
	}
	s1, log1 := run()
	s2, log2 := run()

	if s1.Projection() != s2.Projection() {
		t.Fatalf("projections differ: %d vs %d", s1.Projection(), s2.Projection())
	}
	if len(log1) != len(log2) {
		t.Fatalf("log lengths differ")
	}
	// Same per-event results regardless of delivery order: compare as a map
	// keyed by event ID.
	byID := func(log []DeliveryRecord) map[string]Result {
		m := make(map[string]Result, len(log))
		for _, rec := range log {
			m[rec.Event.ID] = rec.Result
		}
		return m
	}
	m1, m2 := byID(log1), byID(log2)
	for id, r1 := range m1 {
		if m2[id] != r1 {
			t.Fatalf("event %s: result %+v != %+v", id, r1, m2[id])
		}
	}
}
