package lsmkv

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/nikrybak/lsmkv/internal/compaction"
	"github.com/nikrybak/lsmkv/internal/iterator"
	"github.com/nikrybak/lsmkv/internal/manifest"
	"github.com/nikrybak/lsmkv/internal/memtable"
	"github.com/nikrybak/lsmkv/internal/sstable"
)

func (db *DB) flushLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.flushCh:
		case <-db.closing:
			return
		}
		for {
			db.mu.Lock()
			if len(db.imm) == 0 || db.closed || db.bgErr != nil {
				db.mu.Unlock()
				break
			}
			m := db.imm[0]
			db.mu.Unlock()
			if err := db.flush(m); err != nil {
				db.setBGError(fmt.Errorf("lsmkv: flush: %w", err))
				break
			}
		}
	}
}

// flush writes the oldest frozen memtable to level 0, installs it, and drops
// the log that backed it.
func (db *DB) flush(m *memTable) error {
	start := time.Now()
	t, err := db.writeL0(m.Memtable, db.smallestSnapshot())
	if err != nil {
		return err
	}
	edit := manifest.Edit{Added: []manifest.LevelFile{{Level: 0, Meta: t.meta}}}
	err = db.install(edit, []*table{t}, func() {
		db.imm = db.imm[1:]
	}, func() uint64 {
		// Every log older than the next unflushed memtable is now redundant.
		if len(db.imm) > 1 {
			return db.imm[1].logNum
		}
		return db.mem.logNum
	})
	if err != nil {
		db.discard([]*table{t})
		return err
	}
	if err := os.Remove(logPath(db.dir, m.logNum)); err != nil {
		db.opts.logger.Warn("remove flushed log", "log", m.logNum, "err", err)
	}
	db.stats.flushes.Add(1)
	db.stats.bytesFlushed.Add(t.meta.Size)
	db.opts.logger.Info("flushed memtable", "table", t.meta.Num, "entries", m.Len(),
		"bytes", t.meta.Size, "took", time.Since(start))
	db.maybeScheduleCompaction()
	return nil
}

// writeL0 writes a memtable to a single table. Versions no snapshot can see
// are dropped on the way, so a hot key rewritten many times costs one entry.
func (db *DB) writeL0(m *memtable.Memtable, smallestSnapshot uint64) (*table, error) {
	sink := db.newTableSink(math.MaxUint64)
	if _, err := compaction.Run(m.NewIterator(), smallestSnapshot, false, sink); err != nil {
		sink.abort()
		return nil, err
	}
	if err := sink.finish(); err != nil {
		sink.abort()
		return nil, err
	}
	if len(sink.outputs) != 1 {
		sink.abort()
		return nil, fmt.Errorf("lsmkv: flush produced %d tables", len(sink.outputs))
	}
	return sink.outputs[0], nil
}

func (db *DB) compactLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.compactCh:
		case <-db.closing:
			return
		}
		for {
			select {
			case <-db.closing:
				return
			default:
			}
			db.mu.Lock()
			if db.bgErr != nil {
				db.mu.Unlock()
				break
			}
			v := db.current
			v.ref()
			db.mu.Unlock()

			c := db.picker.Pick(v.v)
			if c == nil {
				v.unref()
				break
			}
			err := db.compact(c, v)
			v.unref()
			if err != nil {
				db.setBGError(fmt.Errorf("lsmkv: compaction %v: %w", c, err))
				break
			}
		}
	}
}

func (db *DB) compact(c *compaction.Compaction, v *version) error {
	start := time.Now()
	if c.TrivialMove {
		f := c.Inputs[0][0]
		edit := manifest.Edit{
			Deleted: map[uint64]bool{f.Num: true},
			Added:   []manifest.LevelFile{{Level: c.OutputLevel, Meta: f}},
		}
		if err := db.install(edit, nil, nil, nil); err != nil {
			return err
		}
		db.stats.trivialMoves.Add(1)
		db.opts.logger.Info("moved table", "table", f.Num, "from", c.Level, "to", c.OutputLevel)
		return nil
	}

	var its []iterator.Iterator
	for _, in := range c.Inputs {
		for _, f := range in {
			its = append(its, v.tables[f.Num].r.NewIterator())
		}
	}
	src := iterator.NewMerging(its...)
	sink := db.newTableSink(db.opts.targetFileSize)
	st, err := compaction.Run(src, db.smallestSnapshot(), c.Bottommost, sink)
	if cerr := src.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = sink.finish()
	}
	if err != nil {
		sink.abort()
		return err
	}
	metas := make([]*manifest.FileMeta, len(sink.outputs))
	var written uint64
	for i, t := range sink.outputs {
		metas[i] = t.meta
		written += t.meta.Size
	}
	if err := db.install(c.Edit(metas), sink.outputs, nil, nil); err != nil {
		db.discard(sink.outputs)
		return err
	}
	db.stats.compactions.Add(1)
	db.stats.bytesCompacted.Add(written)
	db.opts.logger.Info("compacted", "job", c.String(), "in", st.In, "out", st.Out,
		"shadowed", st.DroppedShadowed, "tombstones", st.DroppedTombstones,
		"tables", len(metas), "bytes", written, "took", time.Since(start))
	return nil
}

// install applies edit to the current version, persists the result in the
// manifest, and swaps it in. onInstall runs under db.mu together with the
// swap; nextLog, if set, computes the new log number under db.mu.
//
// The manifest is written without db.mu held, so readers are never blocked
// on its fsync; installMu keeps installs (flush and compaction) ordered.
func (db *DB) install(edit manifest.Edit, added []*table, onInstall func(), nextLog func() uint64) error {
	db.installMu.Lock()
	defer db.installMu.Unlock()

	db.mu.Lock()
	cur := db.current
	logNum := db.logNum
	if nextLog != nil {
		logNum = nextLog()
	}
	db.mu.Unlock()

	nv, err := cur.v.Apply(edit)
	if err != nil {
		return err
	}
	err = manifest.Store(db.dir, manifest.State{
		NextFileNum: db.nextFileNum.Load(),
		LogNum:      logNum,
		LastSeq:     db.visibleSeq.Load(),
		Version:     nv,
	})
	if err != nil {
		return err
	}
	db.logNum = logNum

	tables := make(map[uint64]*table)
	for _, t := range added {
		tables[t.meta.Num] = t
	}
	for num := range nv.Files() {
		if t, ok := cur.tables[num]; ok {
			tables[num] = t
		}
	}
	next := newVersion(nv, tables)

	db.mu.Lock()
	db.current = next
	if onInstall != nil {
		onInstall()
	}
	db.cond.Broadcast()
	db.mu.Unlock()
	cur.unref()
	return nil
}

// smallestSnapshot is the oldest sequence number any reader may still ask
// for. Versions shadowed at or below it are invisible to everyone.
func (db *DB) smallestSnapshot() uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	s := db.visibleSeq.Load()
	for seq := range db.snapshots {
		s = min(s, seq)
	}
	return s
}

// discard closes and deletes tables that never made it into a version.
func (db *DB) discard(ts []*table) {
	for _, t := range ts {
		_ = t.r.Close()
		_ = os.Remove(t.path)
	}
}

// tableSink writes compaction or flush output, cutting a new table at a user
// key boundary once the current one reaches the target size.
type tableSink struct {
	db      *DB
	target  uint64
	w       *sstable.Writer
	num     uint64
	outputs []*table
}

func (db *DB) newTableSink(target uint64) *tableSink {
	return &tableSink{db: db, target: target}
}

func (s *tableSink) Boundary() error {
	if s.w != nil && s.w.EstimatedSize() >= s.target {
		return s.finish()
	}
	return nil
}

func (s *tableSink) Add(ikey, value []byte) error {
	if s.w == nil {
		s.num = s.db.newFileNum()
		w, err := sstable.Create(tablePath(s.db.dir, s.num), sstable.WriterOptions{
			BlockSize:       s.db.opts.blockSize,
			BloomBitsPerKey: s.db.opts.bloomBitsPerKey,
		})
		if err != nil {
			return err
		}
		s.w = w
	}
	return s.w.Add(ikey, value)
}

func (s *tableSink) finish() error {
	if s.w == nil {
		return nil
	}
	w := s.w
	s.w = nil
	m, err := w.Finish()
	if err != nil {
		w.Abort()
		return err
	}
	meta := &manifest.FileMeta{Num: s.num, Size: m.Size, Smallest: m.Smallest, Largest: m.Largest}
	t, err := s.db.openTable(meta)
	if err != nil {
		_ = os.Remove(tablePath(s.db.dir, s.num))
		return err
	}
	s.outputs = append(s.outputs, t)
	return nil
}

func (s *tableSink) abort() {
	if s.w != nil {
		s.w.Abort()
		s.w = nil
	}
	s.db.discard(s.outputs)
	s.outputs = nil
}

var _ compaction.Sink = (*tableSink)(nil)
