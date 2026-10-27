package memtable

import (
	"bytes"
	"sync/atomic"

	"github.com/nikrybak/lsmkv/internal/base"
)

// Memtable is a sorted in-memory buffer of recent writes. Add must be called
// from a single goroutine; Get and iterators may run concurrently with it.
type Memtable struct {
	list    *skiplist
	size    atomic.Int64
	entries atomic.Int64
}

// New returns an empty memtable. seed drives node heights; any value works.
func New(seed uint64) *Memtable {
	return &Memtable{list: newSkiplist(seed)}
}

// Add inserts a version of key. key and value are copied.
func (m *Memtable) Add(seq uint64, kind base.Kind, key, value []byte) {
	ikey := base.MakeKey(make([]byte, 0, len(key)+base.TrailerLen), key, seq, kind)
	var v []byte
	if kind == base.KindSet {
		v = bytes.Clone(value)
		if v == nil {
			v = []byte{}
		}
	}
	m.list.insert(ikey, v)
	m.size.Add(int64(len(ikey) + len(v) + nodeOverhead))
	m.entries.Add(1)
}

// Get returns the newest version of key visible at seq. found is false when
// the memtable holds no such version; a tombstone is reported as found with
// kind KindDelete so the caller stops searching older layers.
func (m *Memtable) Get(key []byte, seq uint64) (value []byte, kind base.Kind, found bool) {
	n := m.list.findGE(base.SeekKey(key, seq), nil)
	if n == nil || !bytes.Equal(base.UserKey(n.key), key) {
		return nil, 0, false
	}
	_, kind = base.Trailer(n.key)
	return n.value, kind, true
}

// ApproximateSize returns the memory footprint of the entries in bytes.
func (m *Memtable) ApproximateSize() int64 { return m.size.Load() }

// Len returns the number of entries, counting every version.
func (m *Memtable) Len() int { return int(m.entries.Load()) }

// Empty reports whether the memtable holds no entries.
func (m *Memtable) Empty() bool { return m.entries.Load() == 0 }

// NewIterator returns an iterator over internal keys. It observes entries
// inserted after its creation, so callers filter by sequence number.
func (m *Memtable) NewIterator() *Iterator { return &Iterator{list: m.list} }

// Iterator walks a memtable in internal-key order.
type Iterator struct {
	list *skiplist
	n    *node
}

// First positions the iterator at the smallest key.
func (it *Iterator) First() { it.n = it.list.head.next[0].Load() }

// SeekGE positions the iterator at the first key >= key.
func (it *Iterator) SeekGE(key []byte) { it.n = it.list.findGE(key, nil) }

// Next advances the iterator.
func (it *Iterator) Next() { it.n = it.n.next[0].Load() }

// Valid reports whether the iterator is positioned at an entry.
func (it *Iterator) Valid() bool { return it.n != nil }

// Key returns the current internal key.
func (it *Iterator) Key() []byte { return it.n.key }

// Value returns the current value.
func (it *Iterator) Value() []byte { return it.n.value }

// Err always returns nil: memory iteration cannot fail.
func (it *Iterator) Err() error { return nil }

// Close releases the iterator.
func (it *Iterator) Close() error { it.n = nil; return nil }
