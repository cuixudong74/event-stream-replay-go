package replay

import (
	"context"
	"sync"
)

// Reason 标识一次投递的处理结果，用于区分首次应用与各类重复投递。
type Reason string

const (
	// ReasonApplied 首次应用该事件。
	ReasonApplied Reason = "applied"
	// ReasonDuplicateRetry 同一通道内的重复投递（如重试、重放恢复后重发）。
	ReasonDuplicateRetry Reason = "duplicate_retry"
	// ReasonDuplicateReplay 另一通道已应用过该事件（如实时与重放并发覆盖同一事件）。
	ReasonDuplicateReplay Reason = "duplicate_replay"
)

// Delivery 是一次事件投递，Channel 标识来源通道（如 "live" 或 "replay:<id>"）。
type Delivery struct {
	Event   Event
	Channel string
}

// Result 是下游对一次投递的应答。
type Result struct {
	Applied bool
	Reason  Reason
}

// Sink 是下游接口。实现必须幂等：同一事件（按 Event.ID 判定）多次投递不得重复累计。
type Sink interface {
	Apply(ctx context.Context, d Delivery) (Result, error)
}

// MemSink 是进程内幂等 Sink，按事件 ID 去重，供测试与演示使用。
// State 保存每个事件的派生值（首次应用时由 project 计算）。
type MemSink struct {
	mu      sync.Mutex
	applied map[string]string // eventID -> 首次应用的通道
	state   map[string]int64  // eventID -> 派生值
	project func(Event) int64
}

// NewMemSink 创建幂等 Sink，project 由事件计算派生值（仅在首次应用时调用）。
func NewMemSink(project func(Event) int64) *MemSink {
	return &MemSink{
		applied: make(map[string]string),
		state:   make(map[string]int64),
		project: project,
	}
}

// Apply 实现幂等写入：同一事件 ID 只累计一次，重复投递返回可区分的原因。
func (m *MemSink) Apply(_ context.Context, d Delivery) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.applied[d.Event.ID]; ok {
		if ch == d.Channel {
			return Result{Applied: false, Reason: ReasonDuplicateRetry}, nil
		}
		return Result{Applied: false, Reason: ReasonDuplicateReplay}, nil
	}
	m.applied[d.Event.ID] = d.Channel
	if m.project != nil {
		m.state[d.Event.ID] = m.project(d.Event)
	}
	return Result{Applied: true, Reason: ReasonApplied}, nil
}

// Total 返回下游派生结果的总和（用于与全量重放结果比对）。
func (m *MemSink) Total() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var sum int64
	for _, v := range m.state {
		sum += v
	}
	return sum
}

// Count 返回已应用的去重事件数。
func (m *MemSink) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.applied)
}
