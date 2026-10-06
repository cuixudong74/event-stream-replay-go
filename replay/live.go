package replay

import (
	"context"
	"errors"
	"time"
)

// LiveProcessor 尾随事件存储，把新到达的事件实时投递到下游。
// 它与重放完全独立：使用自己的通道名与检查点，重放不会阻塞或跳过实时事件。
type LiveProcessor struct {
	store    *Store
	ckpts    *CheckpointStore
	sink     Sink
	channel  string
	pollIdle time.Duration
}

// NewLiveProcessor 创建实时处理器。channel 通常为 "live" 或 "live:<下游名>"，
// 同一 sink 的实时通道名必须稳定，以保证跨重放的幂等判定可区分来源。
func NewLiveProcessor(store *Store, ckpts *CheckpointStore, sink Sink, channel string) *LiveProcessor {
	return &LiveProcessor{store: store, ckpts: ckpts, sink: sink, channel: channel, pollIdle: 5 * time.Millisecond}
}

// Progress 返回实时通道的已提交位置。
func (l *LiveProcessor) Progress() (Checkpoint, error) {
	return l.ckpts.Load(l.channel)
}

// Run 持续处理新事件直到 ctx 结束。每条事件投递成功后才推进检查点，
// 因此进程重启后从上次的 Committed+1 继续，不丢不重（依赖 sink 幂等）。
func (l *LiveProcessor) Run(ctx context.Context) error {
	next := int64(0)
	if cp, err := l.ckpts.Load(l.channel); err == nil {
		next = cp.Committed + 1
	} else if !errors.Is(err, ErrNoCheckpoint) {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if next > l.store.Latest() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(l.pollIdle):
				continue
			}
		}
		events := l.store.Read(next, next)
		if len(events) == 0 {
			continue
		}
		if _, err := l.sink.Apply(ctx, Delivery{Event: events[0], Channel: l.channel}); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// 投递失败：不推进检查点，稍后重试同一事件（由 sink 幂等去重）。
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(l.pollIdle):
				continue
			}
		}
		if err := l.ckpts.Save(Checkpoint{Channel: l.channel, Committed: next}); err != nil {
			return err
		}
		next++
	}
}
