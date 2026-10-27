package lsmkv

import (
	"bytes"
	"errors"

	"github.com/nikrybak/lsmkv/internal/base"
	"github.com/nikrybak/lsmkv/internal/iterator"
)

// KeyRange selects the keys a Scan visits: Start inclusive, End exclusive. A
// nil bound is unbounded.
type KeyRange struct {
	Start []byte
	End   []byte
}

// All selects every key.
func All() KeyRange { return KeyRange{} }

// Range selects keys in [start, end).
func Range(start, end []byte) KeyRange { return KeyRange{Start: start, End: end} }

// Prefix selects keys that begin with p.
func Prefix(p []byte) KeyRange { return KeyRange{Start: p, End: prefixEnd(p)} }

// prefixEnd returns the smallest key greater than every key with prefix p,
// or nil if no such key exists (p is empty or all 0xff).
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// Iterator yields live key/value pairs in ascending key order as of the
// moment it was created. It holds a reference on the tables it reads, so it
// must be closed. An Iterator is not safe for concurrent use.
//
//	it, err := db.Scan(lsmkv.Prefix([]byte("user:")))
//	if err != nil { ... }
//	defer it.Close()
//	for it.Next() {
//		use(it.Key(), it.Value())
//	}
//	if err := it.Err(); err != nil { ... }
type Iterator struct {
	src     iterator.Iterator
	v       *version
	seq     uint64
	r       KeyRange
	started bool
	hasKey  bool
	done    bool
	key     []byte
	value   []byte
	err     error
}

// Scan returns an iterator over r as of now.
func (db *DB) Scan(r KeyRange) (*Iterator, error) {
	return db.scan(r, db.visibleSeq.Load())
}

func (db *DB) scan(r KeyRange, seq uint64) (*Iterator, error) {
	mem, imm, v, err := db.readState()
	if err != nil {
		return nil, err
	}
	its := []iterator.Iterator{mem.NewIterator()}
	for i := len(imm) - 1; i >= 0; i-- {
		its = append(its, imm[i].NewIterator())
	}
	its = append(its, v.iterators()...)
	return &Iterator{src: iterator.NewMerging(its...), v: v, seq: seq, r: r}, nil
}

// Next advances to the next live key and reports whether there is one.
func (it *Iterator) Next() bool {
	if it.done {
		return false
	}
	if !it.started {
		it.started = true
		if it.r.Start != nil {
			it.src.SeekGE(base.SeekKey(it.r.Start, base.MaxSeq))
		} else {
			it.src.First()
		}
	}
	// The source may still sit on the key returned last time; every version
	// of that key is skipped below because it compares equal to it.key.
	for ; it.src.Valid(); it.src.Next() {
		k := it.src.Key()
		uk := base.UserKey(k)
		seq, kind := base.Trailer(k)
		if it.r.End != nil && bytes.Compare(uk, it.r.End) >= 0 {
			break
		}
		if seq > it.seq {
			continue
		}
		if it.hasKey && bytes.Equal(uk, it.key) {
			continue
		}
		// First visible version of a new user key: it decides the outcome.
		it.key = append(it.key[:0], uk...)
		it.hasKey = true
		if kind == base.KindDelete {
			continue
		}
		it.value = append(it.value[:0], it.src.Value()...)
		return true
	}
	it.done = true
	it.err = it.src.Err()
	return false
}

// Key returns the current key. It is valid until the next call to Next.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value. It is valid until the next call to Next.
func (it *Iterator) Value() []byte { return it.value }

// Err returns the error, if any, that stopped iteration.
func (it *Iterator) Err() error { return it.err }

// Close releases the iterator. It is safe to call more than once.
func (it *Iterator) Close() error {
	if it.v == nil {
		return nil
	}
	err := it.src.Close()
	it.v.unref()
	it.v = nil
	if !it.done {
		it.done = true
		it.err = ErrClosed
	}
	if errors.Is(err, ErrClosed) {
		return nil
	}
	return err
}
