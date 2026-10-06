package replay

import "sync"

// Event 是事件流中的一条记录。Pos 为追加时分配的单调递增位置（从 0 开始）。
type Event struct {
	Pos     int64
	ID      string
	Kind    string
	Payload []byte
}

// Store 是本地追加式事件存储，支持按位置区间读取。
type Store struct {
	mu     sync.RWMutex
	events []Event
}

// NewStore 创建空的追加存储。
func NewStore() *Store { return &Store{} }

// Append 追加一条事件并返回其位置。Pos 字段由存储分配，调用方无需填写。
func (s *Store) Append(e Event) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.Pos = int64(len(s.events))
	s.events = append(s.events, e)
	return e.Pos
}

// Read 返回位置区间 [from, to]（两端均含）内的全部事件。
// 区间外的部分被裁剪；from > to 或区间完全超出时返回空切片。
func (s *Store) Read(from, to int64) []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if from < 0 {
		from = 0
	}
	if to >= int64(len(s.events)) {
		to = int64(len(s.events)) - 1
	}
	if from > to {
		return nil
	}
	out := make([]Event, to-from+1)
	copy(out, s.events[from:to+1])
	return out
}

// Latest 返回当前最大位置；存储为空时返回 -1。
func (s *Store) Latest() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.events)) - 1
}
