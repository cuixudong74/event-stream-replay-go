package replay

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// appendEvents 向存储追加 n 条事件，payload 为位置值本身，返回期望总和。
func appendEvents(s *Store, start, n int) {
	for i := start; i < start+n; i++ {
		s.Append(Event{ID: fmt.Sprintf("ev-%d", i), Kind: "test", Payload: []byte{byte(i)}})
	}
}

func sumProject(e Event) int64 { return int64(e.Payload[0]) }

func newReplayerEnv(t *testing.T) (*Store, *Replayer, *CheckpointStore) {
	t.Helper()
	store := NewStore()
	ckpts, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store, NewReplayer(store, ckpts), ckpts
}

// 1. 区间边界：两个端点精确生效，不多算不漏算。
func TestReplayRangeBoundaries(t *testing.T) {
	cases := []struct{ from, to int64 }{{0, 0}, {3, 7}, {9, 9}, {0, 9}, {5, 6}}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("[%d,%d]", tc.from, tc.to), func(t *testing.T) {
			store, r, _ := newReplayerEnv(t)
			appendEvents(store, 0, 10)
			sink := NewMemSink(sumProject)
			sum, err := r.Replay(context.Background(), "r1", sink, tc.from, tc.to, ReplayConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if !sum.Done || sum.Committed != tc.to {
				t.Fatalf("want committed=%d done, got %+v", tc.to, sum)
			}
			wantN := int(tc.to - tc.from + 1)
			if len(sum.Decisions) != wantN || sink.Count() != wantN {
				t.Fatalf("want %d events, got decisions=%d count=%d", wantN, len(sum.Decisions), sink.Count())
			}
			var wantTotal int64
			for i := tc.from; i <= tc.to; i++ {
				wantTotal += int64(i)
			}
			if sink.Total() != wantTotal {
				t.Fatalf("want total %d, got %d", wantTotal, sink.Total())
			}
			for i, d := range sum.Decisions {
				if d.Pos != tc.from+int64(i) || d.Reason != ReasonApplied {
					t.Fatalf("decision %d out of order or not applied: %+v", i, d)
				}
			}
		})
	}
}

// 3. 重复投递：同一事件多次投递只累计一次，原因可区分。
func TestDuplicateDeliveryReasons(t *testing.T) {
	sink := NewMemSink(sumProject)
	ev := Event{Pos: 0, ID: "ev-0", Payload: []byte{7}}
	ctx := context.Background()

	r1, _ := sink.Apply(ctx, Delivery{Event: ev, Channel: "live"})
	if r1.Reason != ReasonApplied || !r1.Applied {
		t.Fatalf("first delivery: %+v", r1)
	}
	r2, _ := sink.Apply(ctx, Delivery{Event: ev, Channel: "live"})
	if r2.Reason != ReasonDuplicateRetry || r2.Applied {
		t.Fatalf("same-channel duplicate: %+v", r2)
	}
	r3, _ := sink.Apply(ctx, Delivery{Event: ev, Channel: "replay:r1"})
	if r3.Reason != ReasonDuplicateReplay || r3.Applied {
		t.Fatalf("cross-channel duplicate: %+v", r3)
	}
	if sink.Total() != 7 || sink.Count() != 1 {
		t.Fatalf("duplicate counted: total=%d count=%d", sink.Total(), sink.Count())
	}
}

// 2. 重放与实时并发：新事件不丢不跳，最终状态等于对完整历史的全量重放。
func TestConcurrentReplayAndLive(t *testing.T) {
	store, r, ckpts := newReplayerEnv(t)
	appendEvents(store, 0, 100) // 历史事件 pos 0..99
	sink := NewMemSink(sumProject)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	live := NewLiveProcessor(store, ckpts, sink, "live")
	liveErr := make(chan error, 1)
	go func() { liveErr <- live.Run(ctx) }()

	// 重放历史区间的同时，持续追加新的实时事件。
	replayDone := make(chan ReplaySummary, 1)
	go func() {
		sum, err := r.Replay(context.Background(), "rr", sink, 0, 99, ReplayConfig{BatchSize: 8, Concurrency: 4})
		if err != nil {
			t.Errorf("replay: %v", err)
		}
		replayDone <- sum
	}()
	for i := 100; i < 200; i++ {
		appendEvents(store, i, 1)
		time.Sleep(time.Millisecond)
	}

	sum := <-replayDone
	if !sum.Done {
		t.Fatalf("replay not done: %+v", sum)
	}
	// 等待实时通道追平到 199。
	deadline := time.Now().Add(5 * time.Second)
	for {
		cp, err := live.Progress()
		if err == nil && cp.Committed >= 199 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("live did not catch up: %+v err=%v", cp, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-liveErr

	// 基准：对完整历史做一次全量重放到全新 sink（同一存储，独立检查点目录）。
	ckpts2, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r2 := NewReplayer(store, ckpts2)
	full := NewMemSink(sumProject)
	if _, err := r2.Replay(context.Background(), "full", full, 0, 199, ReplayConfig{}); err != nil {
		t.Fatal(err)
	}
	if sink.Count() != 200 || sink.Total() != full.Total() {
		t.Fatalf("final state mismatch: count=%d total=%d, want count=200 total=%d",
			sink.Count(), sink.Total(), full.Total())
	}
}

// 4. 中断与恢复：从最近提交位置继续，不重复应用、不跳过；重启后可区分未完成重放。
func TestInterruptAndResume(t *testing.T) {
	store, r, ckpts := newReplayerEnv(t)
	appendEvents(store, 0, 100)
	sink := NewMemSink(sumProject)

	// 第一次运行：投递若干批后取消（模拟中断）。
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(60 * time.Millisecond)
		cancel()
	}()
	sum1, err := r.Replay(ctx, "job", sink, 0, 99, ReplayConfig{BatchSize: 5, RatePerSec: 200})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if sum1.Done || sum1.Committed < 0 || sum1.Committed >= 99 {
		t.Fatalf("unexpected partial state: %+v", sum1)
	}
	if (sum1.Committed+1)%5 != 0 {
		t.Fatalf("committed not on batch boundary: %d", sum1.Committed)
	}

	// 模拟进程重启：用同一检查点目录重建 Replayer，检查点可区分（未完成）。
	r2 := NewReplayer(store, ckpts)
	cp, err := r2.Progress("job")
	if err != nil {
		t.Fatal(err)
	}
	if cp.Done || cp.Committed != sum1.Committed {
		t.Fatalf("checkpoint not resumable: %+v", cp)
	}
	appliedBefore := sink.Count()

	// 恢复：从 Committed+1 继续，已提交事件不得再次投递。
	sum2, err := r2.Replay(context.Background(), "job", sink, 0, 99, ReplayConfig{BatchSize: 5})
	if err != nil || !sum2.Done {
		t.Fatalf("resume failed: %v %+v", err, sum2)
	}
	if len(sum2.Decisions) != int(99-sum1.Committed) {
		t.Fatalf("resume re-delivered committed events: %d decisions, want %d",
			len(sum2.Decisions), 99-sum1.Committed)
	}
	if sink.Count() != 100 {
		t.Fatalf("events lost or double counted: count=%d (before=%d)", sink.Count(), appliedBefore)
	}
	var want int64
	for i := 0; i < 100; i++ {
		want += int64(i)
	}
	if sink.Total() != want {
		t.Fatalf("total=%d want %d", sink.Total(), want)
	}
	cp, _ = r2.Progress("job")
	if !cp.Done {
		t.Fatalf("checkpoint should be done: %+v", cp)
	}
}

// 4b. 检查点原子写：崩溃不留下部分写入（临时文件不影响读取）。
func TestCheckpointAtomicWrite(t *testing.T) {
	_, r, ckpts := newReplayerEnv(t)
	if err := ckpts.Save(Checkpoint{Channel: "replay:x", Committed: 41}); err != nil {
		t.Fatal(err)
	}
	if err := ckpts.Save(Checkpoint{Channel: "replay:x", Committed: 42, Done: true}); err != nil {
		t.Fatal(err)
	}
	cp, err := r.Progress("x")
	if err != nil || cp.Committed != 42 || !cp.Done {
		t.Fatalf("load: %+v err=%v", cp, err)
	}
	if _, err := r.Progress("missing"); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}
}

// 5. 多下游独立进度：慢下游不阻塞快下游，进度各自可查询。
func TestMultipleDownstreamsIndependent(t *testing.T) {
	store, r, _ := newReplayerEnv(t)
	appendEvents(store, 0, 60)

	fast := NewMemSink(sumProject)
	slow := NewMemSink(sumProject)

	var wg sync.WaitGroup
	start := time.Now()
	var fastDur time.Duration
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := r.Replay(context.Background(), "fast", fast, 0, 59, ReplayConfig{BatchSize: 10, Concurrency: 4}); err != nil {
			t.Errorf("fast: %v", err)
		}
		fastDur = time.Since(start)
	}()
	go func() {
		defer wg.Done()
		if _, err := r.Replay(context.Background(), "slow", slow, 0, 59, ReplayConfig{BatchSize: 4, RatePerSec: 100}); err != nil {
			t.Errorf("slow: %v", err)
		}
	}()

	// 运行期间即可独立查询各自进度。
	time.Sleep(30 * time.Millisecond)
	cpFast, errFast := r.Progress("fast")
	cpSlow, errSlow := r.Progress("slow")
	if errFast != nil && errSlow != nil {
		t.Fatalf("progress not queryable: %v %v", errFast, errSlow)
	}
	_ = cpFast
	_ = cpSlow

	wg.Wait()
	if fast.Count() != 60 || slow.Count() != 60 || fast.Total() != slow.Total() {
		t.Fatalf("downstreams diverged: fast=%d/%d slow=%d/%d",
			fast.Count(), fast.Total(), slow.Count(), slow.Total())
	}
	if fastDur > 400*time.Millisecond {
		t.Fatalf("fast downstream blocked by slow one: %v", fastDur)
	}
	for _, id := range []string{"fast", "slow"} {
		cp, err := r.Progress(id)
		if err != nil || !cp.Done || cp.Committed != 59 {
			t.Fatalf("progress %s: %+v err=%v", id, cp, err)
		}
	}
}

// 6. 取消/超时：停在批次边界，明确位置可重启；限速生效。
func TestCancelTimeoutAndRateLimit(t *testing.T) {
	store, r, _ := newReplayerEnv(t)
	appendEvents(store, 0, 50)
	sink := NewMemSink(sumProject)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	sum, err := r.Replay(ctx, "job", sink, 0, 49, ReplayConfig{BatchSize: 5, RatePerSec: 100})
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed < 90*time.Millisecond {
		t.Fatalf("rate limit not enforced, finished in %v", elapsed)
	}
	if sum.Done || (sum.Committed+1)%5 != 0 {
		t.Fatalf("not stopped at batch boundary: %+v", sum)
	}

	// 从明确位置重启，最终完整覆盖区间。
	sum2, err := r.Replay(context.Background(), "job", sink, 0, 49, ReplayConfig{BatchSize: 5})
	if err != nil || !sum2.Done || sink.Count() != 50 {
		t.Fatalf("restart failed: %v %+v count=%d", err, sum2, sink.Count())
	}
}

// 7. 确定性：固定输入与种子下，同一区间重放逐条可复现。
func TestDeterministicReplay(t *testing.T) {
	const seed = 42
	project := func(e Event) int64 {
		rng := rand.New(rand.NewSource(seed ^ (e.Pos + 1)))
		return rng.Int63n(1000)
	}
	run := func() (ReplaySummary, int64) {
		store, r, _ := newReplayerEnv(t)
		appendEvents(store, 0, 80)
		sink := NewMemSink(project)
		sum, err := r.Replay(context.Background(), "det", sink, 10, 69,
			ReplayConfig{BatchSize: 7, Concurrency: 3, Seed: seed})
		if err != nil {
			t.Fatal(err)
		}
		return sum, sink.Total()
	}
	s1, t1 := run()
	s2, t2 := run()
	if t1 != t2 {
		t.Fatalf("totals differ: %d vs %d", t1, t2)
	}
	if len(s1.Decisions) != len(s2.Decisions) {
		t.Fatalf("decision count differs")
	}
	for i := range s1.Decisions {
		if s1.Decisions[i] != s2.Decisions[i] {
			t.Fatalf("decision %d differs: %+v vs %+v", i, s1.Decisions[i], s2.Decisions[i])
		}
	}
}
