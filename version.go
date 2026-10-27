package lsmkv

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"github.com/nikrybak/lsmkv/internal/base"
	"github.com/nikrybak/lsmkv/internal/iterator"
	"github.com/nikrybak/lsmkv/internal/manifest"
	"github.com/nikrybak/lsmkv/internal/sstable"
)

// table is an open SSTable. refs counts the live versions that contain it;
// when the last such version is released the table is closed and, unless the
// DB is shutting down, its file is deleted. This is how compaction removes
// inputs without ever pulling a file from under a running reader.
type table struct {
	meta *manifest.FileMeta
	r    *sstable.Reader
	path string
	refs atomic.Int32
	db   *DB
}

func (t *table) ref() { t.refs.Add(1) }

func (t *table) unref() {
	if t.refs.Add(-1) != 0 {
		return
	}
	if err := t.r.Close(); err != nil {
		t.db.opts.logger.Warn("close table", "table", t.meta.Num, "err", err)
	}
	if t.db.shutdown.Load() {
		return
	}
	if err := os.Remove(t.path); err != nil {
		t.db.opts.logger.Warn("remove obsolete table", "table", t.meta.Num, "err", err)
	}
}

// version pairs an immutable manifest.Version with the open tables it names.
type version struct {
	v      *manifest.Version
	tables map[uint64]*table
	refs   atomic.Int32
}

func newVersion(v *manifest.Version, tables map[uint64]*table) *version {
	nv := &version{v: v, tables: tables}
	for _, t := range tables {
		t.ref()
	}
	nv.refs.Store(1)
	return nv
}

func (v *version) ref() { v.refs.Add(1) }

func (v *version) unref() {
	if v.refs.Add(-1) != 0 {
		return
	}
	for _, t := range v.tables {
		t.unref()
	}
}

// get searches the tables newest to oldest: level 0 in order, then one
// candidate table per deeper level found by binary search on key ranges.
func (v *version) get(db *DB, key []byte, seq uint64) (value []byte, kind base.Kind, found bool, err error) {
	probe := func(f *manifest.FileMeta) (bool, error) {
		t := v.tables[f.Num]
		if !t.r.MayContain(key) {
			db.stats.bloomSkips.Add(1)
			return false, nil
		}
		value, kind, found, err = t.r.Get(key, seq)
		if err != nil {
			return false, fmt.Errorf("table %06d: %w", f.Num, err)
		}
		return found, nil
	}
	for _, f := range v.v.Levels[0] {
		if !f.Overlaps(key, key) {
			continue
		}
		if ok, err := probe(f); ok || err != nil {
			return value, kind, ok, err
		}
	}
	for l := 1; l < manifest.NumLevels; l++ {
		files := v.v.Levels[l]
		i := sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].LargestUser(), key) >= 0 })
		if i == len(files) || bytes.Compare(files[i].SmallestUser(), key) > 0 {
			continue
		}
		if ok, err := probe(files[i]); ok || err != nil {
			return value, kind, ok, err
		}
	}
	return nil, 0, false, nil
}

// iterators returns one iterator per level-0 table, newest first, followed
// by one lazily opening iterator per deeper level.
func (v *version) iterators() []iterator.Iterator {
	var its []iterator.Iterator
	for _, f := range v.v.Levels[0] {
		its = append(its, v.tables[f.Num].r.NewIterator())
	}
	for l := 1; l < manifest.NumLevels; l++ {
		files := v.v.Levels[l]
		if len(files) == 0 {
			continue
		}
		its = append(its, iterator.NewConcat(len(files),
			func(i int) []byte { return files[i].Largest },
			func(i int) (iterator.Iterator, error) { return v.tables[files[i].Num].r.NewIterator(), nil }))
	}
	return its
}
