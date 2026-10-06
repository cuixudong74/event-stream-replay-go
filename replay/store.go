package replay

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Store is a local append-only event storage backed by a file.
//
// Events are persisted as JSON lines, one per offset, starting at offset 1.
// Appends are serialized and fsynced so a committed event is never lost; a
// torn trailing line (crash mid-append) is ignored on open, so readers never
// observe a partial record.
type Store struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	next    uint64  // next offset to assign
	offsets []int64 // offsets[i] = byte position of event with offset i+1
}

// OpenStore opens (or creates) an append-only store at path.
func OpenStore(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	s := &Store{path: path, f: f, next: 1}
	if err := s.reindex(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// reindex scans the file, records the byte offset of every complete line and
// truncates any torn trailing record left by a crash.
func (s *Store) reindex() error {
	if _, err := s.f.Seek(0, 0); err != nil {
		return err
	}
	s.offsets = s.offsets[:0]
	var pos int64
	sc := bufio.NewScanner(s.f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := sc.Bytes()
		pos += int64(len(line)) + 1 // + newline
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			// Torn trailing record: truncate it and stop.
			return s.f.Truncate(pos - int64(len(line)) - 1)
		}
		if e.Offset != uint64(len(s.offsets))+1 {
			return fmt.Errorf("store: non-contiguous offset %d at index %d", e.Offset, len(s.offsets))
		}
		s.offsets = append(s.offsets, pos-int64(len(line))-1)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan store: %w", err)
	}
	s.next = uint64(len(s.offsets)) + 1
	return nil
}

// Append assigns the next offset to e, persists it and returns the offset.
func (s *Store) Append(e Event) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.Offset = s.next
	data, err := json.Marshal(e)
	if err != nil {
		return 0, fmt.Errorf("encode event: %w", err)
	}
	end, err := s.f.Seek(0, 2)
	if err != nil {
		return 0, err
	}
	if _, err := s.f.Write(append(data, '\n')); err != nil {
		return 0, fmt.Errorf("append event: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return 0, fmt.Errorf("sync store: %w", err)
	}
	s.offsets = append(s.offsets, end)
	s.next++
	return e.Offset, nil
}

// Read returns all events with offsets in the inclusive range [from, to].
// Both endpoints are exact: every event inside the range is returned exactly
// once and nothing outside it is included.
func (s *Store) Read(from, to uint64) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if from > to {
		return nil, fmt.Errorf("store: invalid range [%d, %d]", from, to)
	}
	if from == 0 {
		return nil, fmt.Errorf("store: offsets start at 1")
	}
	if to > uint64(len(s.offsets)) {
		return nil, fmt.Errorf("store: range end %d beyond latest %d", to, len(s.offsets))
	}
	out := make([]Event, 0, to-from+1)
	for off := from; off <= to; off++ {
		if _, err := s.f.Seek(s.offsets[off-1], 0); err != nil {
			return nil, err
		}
		line, err := bufio.NewReader(s.f).ReadBytes('\n')
		if err != nil {
			return nil, fmt.Errorf("read offset %d: %w", off, err)
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("decode offset %d: %w", off, err)
		}
		out = append(out, e)
	}
	return out, nil
}

// Latest returns the highest committed offset, or 0 when the store is empty.
func (s *Store) Latest() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.offsets))
}

// Close closes the underlying file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
