package module

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

// RetentionEvery is how often a retention pass runs after the one that
// follows restore.
const RetentionEvery = time.Hour

// StateFile is the state file's name in BLOGS_STATE_DIR.
const StateFile = "blogs-retention.json"

// Mark is one stream's dropped prefix: intervals 1 to Interval, seqs 1 to
// Seq, and C, the commitment kept at Seq (hash byte order, hex): what
// Seq + 1's prev must equal to be linked.
type Mark struct {
	Producer string `json:"producer"`
	Stream   string `json:"stream"`
	Interval uint64 `json:"interval"`
	Seq      uint64 `json:"seq"`
	C        string `json:"c"`
}

// TreeLow is, for one tree, the lowest funding output whose carrier
// retention dropped. Key is "topic|tree txid".
type TreeLow struct {
	Key  string
	Vout uint32
}

// RetentionState is what the state file holds.
type RetentionState struct {
	Marks []Mark
	Trees []TreeLow
}

// StateStore is where the state lives.
type StateStore interface {
	Load() (RetentionState, error)
	Save(RetentionState) error
}

// Retention is a host's policy: how long a proven carrier is kept, where
// the state is kept, and the host's clock in Unix milliseconds.
type Retention struct {
	KeepMs float64
	Store  StateStore
	Now    func() int64
}

// fileState is the TypeScript module's file, byte compatible: {"v":1,
// "marks":[...],"trees":[["topic|txid",n],...]}.
type fileState struct {
	V     int               `json:"v"`
	Marks []Mark            `json:"marks"`
	Trees []json.RawMessage `json:"trees"`
}

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// FileStore is the state file in Dir: written whole, flushed, then renamed
// over the last.
type FileStore struct{ Dir string }

// Load implements StateStore. A missing file is the empty state; a file
// that does not parse stops the host, because a dropped position read as
// held again would be counted a hole.
func (f FileStore) Load() (RetentionState, error) {
	path := filepath.Join(f.Dir, StateFile)
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return RetentionState{}, nil
	}
	if err != nil {
		return RetentionState{}, err
	}
	bad := fmt.Errorf("%s is not a blogs retention state; a position read as held again would be counted a hole", path)
	var j fileState
	if json.Unmarshal(raw, &j) != nil || j.V != 1 || j.Marks == nil || j.Trees == nil {
		return RetentionState{}, bad
	}
	var st RetentionState
	for _, m := range j.Marks {
		if m.Producer == "" || m.Stream == "" || !hex64.MatchString(m.C) {
			return RetentionState{}, bad
		}
		st.Marks = append(st.Marks, m)
	}
	for _, t := range j.Trees {
		var pair []json.RawMessage
		if json.Unmarshal(t, &pair) != nil || len(pair) != 2 {
			return RetentionState{}, bad
		}
		var k string
		var n uint32
		if json.Unmarshal(pair[0], &k) != nil || json.Unmarshal(pair[1], &n) != nil {
			return RetentionState{}, bad
		}
		st.Trees = append(st.Trees, TreeLow{Key: k, Vout: n})
	}
	return st, nil
}

// Save implements StateStore.
func (f FileStore) Save(s RetentionState) error {
	if err := os.MkdirAll(f.Dir, 0o700); err != nil {
		return err
	}
	j := fileState{V: 1, Marks: s.Marks, Trees: []json.RawMessage{}}
	if j.Marks == nil {
		j.Marks = []Mark{}
	}
	for _, t := range s.Trees {
		b, err := json.Marshal([]any{t.Key, t.Vout})
		if err != nil {
			return err
		}
		j.Trees = append(j.Trees, b)
	}
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	path := filepath.Join(f.Dir, StateFile)
	tmp := path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := fh.Write(b); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// MemoryStore is a StateStore in memory, for tests.
type MemoryStore struct {
	mu    sync.Mutex
	State RetentionState
	Saved int
	// Fail, when set, fails every Save.
	Fail error
}

// Load implements StateStore.
func (m *MemoryStore) Load() (RetentionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return RetentionState{Marks: append([]Mark(nil), m.State.Marks...), Trees: append([]TreeLow(nil), m.State.Trees...)}, nil
}

// Save implements StateStore.
func (m *MemoryStore) Save(s RetentionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail != nil {
		return m.Fail
	}
	m.State = RetentionState{Marks: append([]Mark(nil), s.Marks...), Trees: append([]TreeLow(nil), s.Trees...)}
	m.Saved++
	return nil
}
