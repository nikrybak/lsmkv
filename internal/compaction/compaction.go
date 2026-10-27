// Package compaction decides what to compact (the picker) and how entries are
// rewritten (Run). It knows nothing about files: the engine opens inputs,
// supplies a merged iterator and a Sink that writes output tables.
//
// The strategy is leveled compaction. Level 0 receives flushed memtables and
// may hold overlapping tables; once it has L0Trigger tables, all of them are
// merged with the overlapping part of level 1. Every deeper level holds
// disjoint tables and has a size budget BaseLevelSize * Multiplier^(level-1);
// the level most over budget pushes one table into the next level. A key is
// therefore stored in at most one table per level >= 1, which bounds read
// amplification at (L0 tables + levels) and space amplification at about
// 1.1x, at the cost of higher write amplification than size-tiered schemes.
package compaction

import (
	"bytes"
	"fmt"

	"github.com/nikrybak/lsmkv/internal/base"
	"github.com/nikrybak/lsmkv/internal/iterator"
	"github.com/nikrybak/lsmkv/internal/manifest"
)

// Options configures the picker.
type Options struct {
	L0Trigger     int    // level-0 table count that triggers a compaction
	BaseLevelSize uint64 // size budget of level 1 in bytes
	Multiplier    uint64 // growth factor between consecutive levels
}

// Compaction is one unit of work chosen by the picker.
type Compaction struct {
	Level       int
	OutputLevel int
	Inputs      [2][]*manifest.FileMeta // tables from Level and OutputLevel
	// Bottommost is true when no deeper level overlaps the key range, so
	// tombstones visible to every snapshot have nothing left to shadow.
	Bottommost bool
	// TrivialMove is true when the single input can be relinked into the
	// next level without being rewritten.
	TrivialMove bool
}

// Edit returns the manifest edit that installs outputs in place of the
// inputs.
func (c *Compaction) Edit(outputs []*manifest.FileMeta) manifest.Edit {
	e := manifest.Edit{Deleted: make(map[uint64]bool)}
	for _, in := range c.Inputs {
		for _, f := range in {
			e.Deleted[f.Num] = true
		}
	}
	for _, f := range outputs {
		e.Added = append(e.Added, manifest.LevelFile{Level: c.OutputLevel, Meta: f})
	}
	return e
}

// String implements fmt.Stringer for logging.
func (c *Compaction) String() string {
	return fmt.Sprintf("L%d(%d tables) + L%d(%d tables) -> L%d",
		c.Level, len(c.Inputs[0]), c.OutputLevel, len(c.Inputs[1]), c.OutputLevel)
}

// Picker chooses compactions. It remembers, per level, where the previous
// compaction stopped so that successive compactions sweep the key space in a
// round-robin fashion. It is not safe for concurrent use; the engine runs a
// single compaction goroutine.
type Picker struct {
	opts    Options
	pointer [manifest.NumLevels][]byte // last compacted user key per level
}

// NewPicker returns a picker with opts, filling in defaults.
func NewPicker(opts Options) *Picker {
	if opts.L0Trigger <= 0 {
		opts.L0Trigger = 4
	}
	if opts.BaseLevelSize == 0 {
		opts.BaseLevelSize = 10 << 20
	}
	if opts.Multiplier < 2 {
		opts.Multiplier = 10
	}
	return &Picker{opts: opts}
}

// MaxBytes returns the size budget of level (level >= 1).
func (p *Picker) MaxBytes(level int) uint64 {
	n := p.opts.BaseLevelSize
	for l := 1; l < level; l++ {
		n *= p.opts.Multiplier
	}
	return n
}

// Score returns how urgently level needs compaction; >= 1 means it does.
func (p *Picker) Score(v *manifest.Version, level int) float64 {
	if level == 0 {
		return float64(len(v.Levels[0])) / float64(p.opts.L0Trigger)
	}
	return float64(v.LevelSize(level)) / float64(p.MaxBytes(level))
}

// Pick returns the most urgent compaction, or nil when every level is within
// budget.
func (p *Picker) Pick(v *manifest.Version) *Compaction {
	bestLevel, bestScore := -1, 1.0
	// The last level has nowhere to push data, so it is never a source.
	for l := 0; l < manifest.NumLevels-1; l++ {
		if s := p.Score(v, l); s >= bestScore {
			bestLevel, bestScore = l, s
		}
	}
	if bestLevel < 0 {
		return nil
	}
	c := &Compaction{Level: bestLevel, OutputLevel: bestLevel + 1}
	if bestLevel == 0 {
		c.Inputs[0] = append(c.Inputs[0], v.Levels[0]...)
	} else {
		c.Inputs[0] = []*manifest.FileMeta{p.nextFile(v, bestLevel)}
	}
	lo, hi := keyRange(c.Inputs[0])
	c.Inputs[1] = v.Overlapping(c.OutputLevel, lo, hi)
	if c.Level > 0 {
		p.pointer[c.Level] = bytes.Clone(hi)
	}
	allLo, allHi := keyRange(append(append([]*manifest.FileMeta(nil), c.Inputs[0]...), c.Inputs[1]...))
	c.Bottommost = true
	for l := c.OutputLevel + 1; l < manifest.NumLevels; l++ {
		if len(v.Overlapping(l, allLo, allHi)) > 0 {
			c.Bottommost = false
			break
		}
	}
	c.TrivialMove = c.Level > 0 && len(c.Inputs[0]) == 1 && len(c.Inputs[1]) == 0
	return c
}

func (p *Picker) nextFile(v *manifest.Version, level int) *manifest.FileMeta {
	files := v.Levels[level]
	if ptr := p.pointer[level]; ptr != nil {
		for _, f := range files {
			if bytes.Compare(f.SmallestUser(), ptr) > 0 {
				return f
			}
		}
	}
	return files[0]
}

func keyRange(files []*manifest.FileMeta) (lo, hi []byte) {
	for _, f := range files {
		if lo == nil || bytes.Compare(f.SmallestUser(), lo) < 0 {
			lo = f.SmallestUser()
		}
		if hi == nil || bytes.Compare(f.LargestUser(), hi) > 0 {
			hi = f.LargestUser()
		}
	}
	return lo, hi
}

// Sink receives the entries a compaction keeps.
type Sink interface {
	// Boundary is called before the first kept version of every user key.
	// It is the only point where the sink may cut a new output table, which
	// keeps all versions of a key in one table and the level disjoint.
	Boundary() error
	Add(ikey, value []byte) error
}

// Stats summarises one Run.
type Stats struct {
	In                int
	Out               int
	DroppedShadowed   int
	DroppedTombstones int
}

// Run drains src, which must be positioned by Run itself, and forwards to
// sink every entry that some reader could still observe.
//
// For each user key the versions arrive newest first. A version is dropped
// when a newer version of the same key is already visible to the oldest live
// snapshot (no reader can reach past it), or when it is a tombstone visible to
// every snapshot in a bottommost compaction (there is nothing below it to
// hide).
func Run(src iterator.Iterator, smallestSnapshot uint64, bottommost bool, sink Sink) (Stats, error) {
	var (
		st      Stats
		curUK   []byte
		started bool
		prevSeq uint64
		emitted bool
	)
	for src.First(); src.Valid(); src.Next() {
		k := src.Key()
		uk := base.UserKey(k)
		seq, kind := base.Trailer(k)
		st.In++
		if !started || !bytes.Equal(uk, curUK) {
			curUK = append(curUK[:0], uk...)
			started = true
			emitted = false
			prevSeq = base.MaxSeq + 1
		}
		shadowed := prevSeq <= smallestSnapshot
		prevSeq = seq
		switch {
		case shadowed:
			st.DroppedShadowed++
			continue
		case kind == base.KindDelete && bottommost && seq <= smallestSnapshot:
			st.DroppedTombstones++
			continue
		}
		if !emitted {
			if err := sink.Boundary(); err != nil {
				return st, err
			}
			emitted = true
		}
		if err := sink.Add(k, src.Value()); err != nil {
			return st, err
		}
		st.Out++
	}
	return st, src.Err()
}
