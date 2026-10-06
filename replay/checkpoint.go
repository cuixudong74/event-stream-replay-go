package replay

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// ReplayStatus describes the lifecycle state of a replay task.
type ReplayStatus string

const (
	// StatusRunning means the replay is in progress (or was interrupted mid-run).
	StatusRunning ReplayStatus = "running"
	// StatusDone means the replay covered its full range.
	StatusDone ReplayStatus = "done"
	// StatusCanceled means the replay was canceled and can be resumed from Next.
	StatusCanceled ReplayStatus = "canceled"
)

// ReplayState records the durable progress of one replay task.
type ReplayState struct {
	From   uint64       `json:"from"`
	To     uint64       `json:"to"`
	Next   uint64       `json:"next"` // next offset to apply; everything below Next is committed
	Status ReplayStatus `json:"status"`
}

// Checkpoint is the durable per-downstream progress. LiveCommitted is the
// committed live position; Replay, when non-nil, is an unfinished or finished
// replay task. The two are always distinguishable after a restart.
type Checkpoint struct {
	LiveCommitted uint64       `json:"live_committed"`
	Replay        *ReplayState `json:"replay,omitempty"`
}

// CheckpointStore persists checkpoints atomically (no partial writes): every
// save goes to a temporary file which is fsynced and then renamed over the
// target, so a crash can never leave a half-written checkpoint behind.
type CheckpointStore struct {
	mu  sync.Mutex
	dir string
}

// NewCheckpointStore creates a store persisting checkpoints under dir.
func NewCheckpointStore(dir string) *CheckpointStore {
	return &CheckpointStore{dir: dir}
}

func (s *CheckpointStore) path(name string) string {
	return filepath.Join(s.dir, name+".checkpoint.json")
}

// Load reads the checkpoint for a downstream; a zero Checkpoint is returned
// when none exists yet.
func (s *CheckpointStore) Load(name string) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(name)
}

func (s *CheckpointStore) loadLocked(name string) (Checkpoint, error) {
	data, err := os.ReadFile(s.path(name))
	if errors.Is(err, fs.ErrNotExist) {
		return Checkpoint{}, nil
	}
	if err != nil {
		return Checkpoint{}, fmt.Errorf("load checkpoint %q: %w", name, err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, fmt.Errorf("decode checkpoint %q: %w", name, err)
	}
	return cp, nil
}

// Update atomically applies fn to the current checkpoint of a downstream and
// persists the result. The read-modify-write happens under the store lock, so
// concurrent writers (e.g. a live processor and a replayer of the same
// downstream) never clobber each other's fields.
func (s *CheckpointStore) Update(name string, fn func(Checkpoint) Checkpoint) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, err := s.loadLocked(name)
	if err != nil {
		return Checkpoint{}, err
	}
	cp = fn(cp)
	if err := s.saveLocked(name, cp); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}

// Save atomically persists the checkpoint for a downstream.
func (s *CheckpointStore) Save(name string, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked(name, cp)
}

func (s *CheckpointStore) saveLocked(name string, cp Checkpoint) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(name) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("save checkpoint %q: %w", name, err)
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
	if err := os.Rename(tmp, s.path(name)); err != nil {
		return fmt.Errorf("commit checkpoint %q: %w", name, err)
	}
	return nil
}
