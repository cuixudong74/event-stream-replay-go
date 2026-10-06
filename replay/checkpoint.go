package replay

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Checkpoint 记录某个通道（实时或某次重放）的提交进度。
// Committed 表示已提交的最大位置（下一条从 Committed+1 开始），-1 表示尚未提交任何事件。
// Done 区分"进行中的重放"与"已完成/不存在的重放"，进程重启后据此判断能否恢复。
type Checkpoint struct {
	Channel   string `json:"channel"`
	Committed int64  `json:"committed"`
	Done      bool   `json:"done"`
}

// CheckpointStore 以文件持久化各通道的检查点。
// 写入采用"临时文件 + fsync + rename"，保证崩溃后要么看到旧值要么看到新值，不留部分写入。
type CheckpointStore struct {
	mu  sync.Mutex
	dir string
}

// NewCheckpointStore 创建基于目录的检查点存储，目录不存在时自动创建。
func NewCheckpointStore(dir string) (*CheckpointStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &CheckpointStore{dir: dir}, nil
}

var ErrNoCheckpoint = errors.New("replay: no checkpoint for channel")

func (c *CheckpointStore) path(channel string) string {
	return filepath.Join(c.dir, channel+".json")
}

// Load 读取通道的检查点；不存在时返回 ErrNoCheckpoint。
func (c *CheckpointStore) Load(channel string) (Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := os.ReadFile(c.path(channel))
	if errors.Is(err, os.ErrNotExist) {
		return Checkpoint{}, ErrNoCheckpoint
	}
	if err != nil {
		return Checkpoint{}, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, fmt.Errorf("replay: corrupt checkpoint for %q: %w", channel, err)
	}
	return cp, nil
}

// Save 原子写入通道的检查点。
func (c *CheckpointStore) Save(cp Checkpoint) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	tmp := c.path(cp.Channel) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, c.path(cp.Channel))
}
