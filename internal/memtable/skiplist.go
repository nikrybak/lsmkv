// Package memtable implements the in-memory write buffer as a skiplist keyed
// by internal key.
//
// Concurrency model: exactly one goroutine inserts, any number read. Readers
// never take a lock. The writer fully initialises a node, then publishes it by
// storing it into its predecessors' next pointers from the bottom level up.
// Because every pointer is an atomic.Pointer, a reader either sees the new
// node with all of its fields visible or does not see it at all, and the
// level-0 list, which defines membership, is always complete.
package memtable

import (
	"math/rand/v2"
	"sync/atomic"

	"github.com/nikrybak/lsmkv/internal/base"
)

const (
	maxHeight = 12
	// A branching factor of 4 gives about 1.33 pointers per node, a better
	// memory/search trade-off than 2 for keys that are compared byte-wise.
	branching = 4
	// nodeOverhead approximates the per-entry bookkeeping cost so that size
	// accounting tracks real memory and not just payload bytes.
	nodeOverhead = 48
)

type node struct {
	key   []byte
	value []byte
	next  []atomic.Pointer[node]
}

type skiplist struct {
	head   *node
	height atomic.Int32
	rng    *rand.Rand // touched only by the writer
}

func newSkiplist(seed uint64) *skiplist {
	s := &skiplist{
		head: &node{next: make([]atomic.Pointer[node], maxHeight)},
		rng:  rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
	s.height.Store(1)
	return s
}

func (s *skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && s.rng.IntN(branching) == 0 {
		h++
	}
	return h
}

// findGE returns the first node with key >= target. When prev is non-nil it
// is filled with the rightmost node before target at every level.
func (s *skiplist) findGE(target []byte, prev *[maxHeight]*node) *node {
	x := s.head
	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		for {
			next := x.next[level].Load()
			if next == nil || base.Compare(next.key, target) >= 0 {
				break
			}
			x = next
		}
		if prev != nil {
			prev[level] = x
		}
	}
	return x.next[0].Load()
}

// insert adds key. Internal keys are unique because every write carries a
// fresh sequence number, so no duplicate handling is needed.
func (s *skiplist) insert(key, value []byte) {
	var prev [maxHeight]*node
	s.findGE(key, &prev)

	h := s.randomHeight()
	if cur := int(s.height.Load()); h > cur {
		for i := cur; i < h; i++ {
			prev[i] = s.head
		}
		// Raising the height before linking is safe: readers that observe
		// the new height simply find nil at the new levels of head.
		s.height.Store(int32(h))
	}
	n := &node{key: key, value: value, next: make([]atomic.Pointer[node], h)}
	for i := 0; i < h; i++ {
		n.next[i].Store(prev[i].next[i].Load())
		prev[i].next[i].Store(n)
	}
}
