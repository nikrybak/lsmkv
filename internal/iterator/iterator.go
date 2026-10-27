// Package iterator defines the internal iterator contract and the
// combinators built on it: a k-way merging iterator and a lazy concatenating
// iterator for non-overlapping sorted runs.
package iterator

import (
	"container/heap"
	"errors"
	"sort"

	"github.com/nikrybak/lsmkv/internal/base"
)

// Iterator walks internal keys in ascending base.Compare order. Key and Value
// are only valid until the next positioning call; callers that keep them must
// copy. Implementations: memtable.Iterator, sstable.Iterator, Merging, Concat.
type Iterator interface {
	First()
	SeekGE(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Err() error
	Close() error
}

// Merging merges any number of sorted children into one sorted stream. When
// two children hold an equal key the one passed first wins, which lets the
// caller list sources newest first.
type Merging struct {
	children []Iterator
	h        mergeHeap
	err      error
}

// NewMerging returns a merging iterator. It takes ownership of children.
func NewMerging(children ...Iterator) *Merging {
	m := &Merging{children: children}
	m.h.children = children
	return m
}

func (m *Merging) rebuild() {
	m.h.idx = m.h.idx[:0]
	m.err = nil
	for i, c := range m.children {
		if c.Valid() {
			m.h.idx = append(m.h.idx, i)
		} else if err := c.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}
	heap.Init(&m.h)
}

// First positions every child at its first key.
func (m *Merging) First() {
	for _, c := range m.children {
		c.First()
	}
	m.rebuild()
}

// SeekGE positions every child at its first key >= key.
func (m *Merging) SeekGE(key []byte) {
	for _, c := range m.children {
		c.SeekGE(key)
	}
	m.rebuild()
}

// Next advances the child holding the smallest key.
func (m *Merging) Next() {
	if !m.Valid() {
		return
	}
	top := m.children[m.h.idx[0]]
	top.Next()
	if top.Valid() {
		heap.Fix(&m.h, 0)
		return
	}
	if err := top.Err(); err != nil {
		m.err = err
	}
	heap.Pop(&m.h)
}

// Valid reports whether the iterator is positioned at an entry. Any child
// error makes the merged stream invalid: silently skipping a broken source
// could surface a shadowed value.
func (m *Merging) Valid() bool { return m.err == nil && len(m.h.idx) > 0 }

// Key returns the smallest current key.
func (m *Merging) Key() []byte { return m.children[m.h.idx[0]].Key() }

// Value returns the value for Key.
func (m *Merging) Value() []byte { return m.children[m.h.idx[0]].Value() }

// Err returns the first child error.
func (m *Merging) Err() error { return m.err }

// Close closes every child.
func (m *Merging) Close() error {
	var errs []error
	for _, c := range m.children {
		errs = append(errs, c.Close())
	}
	m.h.idx = nil
	return errors.Join(errs...)
}

type mergeHeap struct {
	children []Iterator
	idx      []int
}

func (h *mergeHeap) Len() int { return len(h.idx) }
func (h *mergeHeap) Less(i, j int) bool {
	a, b := h.idx[i], h.idx[j]
	if c := base.Compare(h.children[a].Key(), h.children[b].Key()); c != 0 {
		return c < 0
	}
	return a < b
}
func (h *mergeHeap) Swap(i, j int) { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *mergeHeap) Push(x any)    { h.idx = append(h.idx, x.(int)) }
func (h *mergeHeap) Pop() any {
	n := len(h.idx) - 1
	x := h.idx[n]
	h.idx = h.idx[:n]
	return x
}

// Concat iterates a sequence of sorted, non-overlapping runs (the files of a
// level >= 1) and opens each run only when the iterator reaches it.
type Concat struct {
	n       int
	largest func(i int) []byte
	open    func(i int) (Iterator, error)
	cur     Iterator
	pos     int
	err     error
}

// NewConcat returns a Concat over n runs. largest(i) is the largest internal
// key of run i; open(i) opens run i.
func NewConcat(n int, largest func(i int) []byte, open func(i int) (Iterator, error)) *Concat {
	return &Concat{n: n, largest: largest, open: open, pos: -1}
}

func (c *Concat) closeCur() {
	if c.cur != nil {
		if err := c.cur.Close(); err != nil && c.err == nil {
			c.err = err
		}
		c.cur = nil
	}
}

func (c *Concat) openAt(i int) bool {
	c.closeCur()
	c.pos = i
	if i >= c.n || c.err != nil {
		return false
	}
	it, err := c.open(i)
	if err != nil {
		c.err = err
		return false
	}
	c.cur = it
	return true
}

// skipExhausted moves to the next run while the current one is exhausted.
func (c *Concat) skipExhausted() {
	for c.cur != nil && !c.cur.Valid() {
		if err := c.cur.Err(); err != nil {
			c.err = err
			c.closeCur()
			return
		}
		if !c.openAt(c.pos + 1) {
			return
		}
		c.cur.First()
	}
}

// First positions the iterator at the first key of the first run.
func (c *Concat) First() {
	if c.openAt(0) {
		c.cur.First()
		c.skipExhausted()
	}
}

// SeekGE binary-searches the run that may hold key and seeks inside it.
func (c *Concat) SeekGE(key []byte) {
	i := sort.Search(c.n, func(i int) bool { return base.Compare(c.largest(i), key) >= 0 })
	if c.openAt(i) {
		c.cur.SeekGE(key)
		c.skipExhausted()
	}
}

// Next advances the iterator.
func (c *Concat) Next() {
	if !c.Valid() {
		return
	}
	c.cur.Next()
	c.skipExhausted()
}

// Valid reports whether the iterator is positioned at an entry.
func (c *Concat) Valid() bool { return c.err == nil && c.cur != nil && c.cur.Valid() }

// Key returns the current key.
func (c *Concat) Key() []byte { return c.cur.Key() }

// Value returns the current value.
func (c *Concat) Value() []byte { return c.cur.Value() }

// Err returns the first error.
func (c *Concat) Err() error { return c.err }

// Close closes the open run.
func (c *Concat) Close() error {
	c.closeCur()
	return c.err
}
