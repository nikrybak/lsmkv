package lsmkv

import "sync/atomic"

// Snapshot is a consistent, read-only view of the database at the moment it
// was taken. Writes made afterwards are invisible to it. While a snapshot is
// live, compaction keeps every version it can see, so long-lived snapshots
// cost disk space; release them when done.
type Snapshot struct {
	db       *DB
	seq      uint64
	released atomic.Bool
}

// Snapshot captures the current state of the database.
func (db *DB) Snapshot() (*Snapshot, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	seq := db.visibleSeq.Load()
	db.snapshots[seq]++
	return &Snapshot{db: db, seq: seq}, nil
}

// Get returns the value of key as of the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.released.Load() {
		return nil, ErrClosed
	}
	return s.db.get(key, s.seq)
}

// Scan iterates r as of the snapshot.
func (s *Snapshot) Scan(r KeyRange) (*Iterator, error) {
	if s.released.Load() {
		return nil, ErrClosed
	}
	return s.db.scan(r, s.seq)
}

// Sequence returns the sequence number the snapshot reads at.
func (s *Snapshot) Sequence() uint64 { return s.seq }

// Release frees the snapshot. It is safe to call more than once.
func (s *Snapshot) Release() {
	if !s.released.CompareAndSwap(false, true) {
		return
	}
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if s.db.snapshots[s.seq]--; s.db.snapshots[s.seq] <= 0 {
		delete(s.db.snapshots, s.seq)
	}
}
