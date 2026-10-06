package replay

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ReplayConfig 控制单次重放的速率与并发度。
type ReplayConfig struct {
	// RatePerSec 每秒最多投递的事件数；<=0 表示不限速。
	RatePerSec float64
	// Concurrency 单批次内并发投递的协程数；<=0 按 1 处理。
	Concurrency int
	// BatchSize 提交粒度：每处理完一批才推进检查点；<=0 按 64 处理。
	BatchSize int
	// Seed 随机种子。重放本身对固定区间是确定性的；
	// Seed 供需要随机性的投影逻辑派生"逐事件确定"的随机源（如 seed^pos），保证可复现。
	Seed int64
}

func (c ReplayConfig) withDefaults() ReplayConfig {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 64
	}
	return c
}

// Decision 记录一条事件的重放判定结论，按位置有序，可用于复现比对。
type Decision struct {
	Pos     int64
	EventID string
	Reason  Reason
}

// ReplaySummary 是重放结束（含取消）后的结果。
type ReplaySummary struct {
	ReplayID  string
	From, To  int64
	Committed int64      // 本次结束时的已提交位置
	Done      bool       // 是否覆盖了整个区间
	Decisions []Decision // 本次运行逐条判定（按位置升序）
}

// Replayer 对同一份事件流执行区间重放。Replayer 本身无状态，
// 多个下游可各自用不同 replayID 并发重放，进度互不干扰。
type Replayer struct {
	store *Store
	ckpts *CheckpointStore
}

// NewReplayer 创建重放器。
func NewReplayer(store *Store, ckpts *CheckpointStore) *Replayer {
	return &Replayer{store: store, ckpts: ckpts}
}

// Progress 查询某次重放的持久化进度。
func (r *Replayer) Progress(replayID string) (Checkpoint, error) {
	return r.ckpts.Load(replayChannel(replayID))
}

func replayChannel(replayID string) string { return "replay:" + replayID }

// Replay 重放区间 [from, to]（两端均含）内的全部事件并投递到 sink。
//
//   - 幂等恢复：若该 replayID 存在未完成的检查点，从 Committed+1 继续，
//     不重复应用已提交事件，也不跳过任何事件。
//   - 取消/超时：ctx 结束时在批次边界停止，已提交位置持久化，
//     下游不会因幂等投递而停在中间状态；用同一 replayID 再次调用即可从明确位置继续。
//   - 确定性：同一区间、同一 Seed 的重放，Decisions 逐条一致。
func (r *Replayer) Replay(ctx context.Context, replayID string, sink Sink, from, to int64, cfg ReplayConfig) (ReplaySummary, error) {
	cfg = cfg.withDefaults()
	channel := replayChannel(replayID)
	sum := ReplaySummary{ReplayID: replayID, From: from, To: to, Committed: from - 1}

	// 恢复：已有未完成检查点时从 Committed+1 继续；已完成则直接返回。
	if cp, err := r.ckpts.Load(channel); err == nil {
		switch {
		case cp.Done:
			sum.Committed = cp.Committed
			sum.Done = true
			return sum, nil
		case cp.Committed >= from-1:
			sum.Committed = cp.Committed
		}
	}

	var limiter <-chan time.Time
	if cfg.RatePerSec > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / cfg.RatePerSec))
		defer t.Stop()
		limiter = t.C
	}

	for sum.Committed < to {
		// 取下一批（批次边界即提交边界，取消不会留下部分提交的批次）。
		batchFrom := sum.Committed + 1
		batchTo := batchFrom + int64(cfg.BatchSize) - 1
		if batchTo > to {
			batchTo = to
		}
		events := r.store.Read(batchFrom, batchTo)
		if len(events) == 0 {
			// 区间内有位置尚未写入（实时尚未到达），等待后重试或响应取消。
			select {
			case <-ctx.Done():
				return sum, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}

		results := make([]Result, len(events))
		if err := deliverBatch(ctx, sink, channel, events, results, cfg.Concurrency, limiter); err != nil {
			// 取消/超时：本批次未提交，检查点仍停留在上一批次末尾，
			// 恢复时重发本批，由 sink 幂等去重，不会重复累计。
			return sum, err
		}
		for i, ev := range events {
			sum.Decisions = append(sum.Decisions, Decision{Pos: ev.Pos, EventID: ev.ID, Reason: results[i].Reason})
		}

		// 批次全部投递成功后原子推进检查点。
		sum.Committed = events[len(events)-1].Pos
		done := sum.Committed >= to
		if err := r.ckpts.Save(Checkpoint{Channel: channel, Committed: sum.Committed, Done: done}); err != nil {
			return sum, fmt.Errorf("replay: save checkpoint: %w", err)
		}
		if done {
			sum.Done = true
		}
	}
	return sum, nil
}

// deliverBatch 并发投递一批事件，结果按下标回填（与位置顺序一致），保证逐条可复现。
func deliverBatch(ctx context.Context, sink Sink, channel string, events []Event, results []Result, concurrency int, limiter <-chan time.Time) error {
	if concurrency > len(events) {
		concurrency = len(events)
	}
	idx := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				if limiter != nil {
					select {
					case <-ctx.Done():
						return
					case <-limiter:
					}
				}
				res, err := sink.Apply(ctx, Delivery{Event: events[i], Channel: channel})
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				results[i] = res
			}
		}()
	}
	send := 0
sendLoop:
	for ; send < len(events); send++ {
		select {
		case <-ctx.Done():
			break sendLoop
		case err := <-errCh:
			close(idx)
			wg.Wait()
			return err
		case idx <- send:
		}
	}
	close(idx)
	wg.Wait()
	if send < len(events) {
		return ctx.Err()
	}
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
