// Package lsmkv is an embeddable key-value store built on a log-structured
// merge tree.
//
// Writes go to a write-ahead log and an in-memory skiplist. Full memtables
// are flushed to immutable SSTables in level 0, and a background compactor
// merges tables down a leveled hierarchy. Reads consult the memtables and
// then the tables newest to oldest, skipping tables through bloom filters.
//
// # Concurrency
//
// A DB is safe for concurrent use. All writes funnel into a single writer
// goroutine that batches whatever is queued (group commit), appends it to the
// log with one fsync, and applies it to the memtable. Readers never block on
// writers or on disk I/O done by background work: they take db.mu only to
// grab a reference to the current memtables and table set, then search them
// without locks. The skiplist supports one writer with lock-free readers, and
// table sets are immutable, reference-counted versions. Flushes and
// compactions build new tables without holding db.mu and install them under
// a separate install lock that serialises manifest writes.
package lsmkv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nikrybak/lsmkv/internal/base"
	"github.com/nikrybak/lsmkv/internal/compaction"
	"github.com/nikrybak/lsmkv/internal/manifest"
	"github.com/nikrybak/lsmkv/internal/memtable"
	"github.com/nikrybak/lsmkv/internal/sstable"
	"github.com/nikrybak/lsmkv/internal/wal"
)

// memTable is a memtable together with the log file that backs it.
type memTable struct {
	*memtable.Memtable
	logNum uint64
}

// DB is an open database. Create one with Open and release it with Close.
type DB struct {
	dir  string
	opts options

	// mu guards the fields below. It is never held across disk I/O.
	mu        sync.Mutex
	cond      *sync.Cond // broadcast when imm shrinks, a version is installed, or on close
	mem       *memTable
	imm       []*memTable // frozen memtables, oldest first; never mutated in place
	current   *version
	snapshots map[uint64]int // sequence number -> live snapshot count
	bgErr     error
	closed    bool

	// installMu serialises version installs and therefore manifest writes.
	installMu sync.Mutex
	logNum    uint64 // guarded by installMu

	nextFileNum atomic.Uint64
	visibleSeq  atomic.Uint64 // highest sequence readers may observe
	shutdown    atomic.Bool

	// Owned by the writer goroutine.
	wal     *wal.Writer
	lastSeq uint64

	picker *compaction.Picker // owned by the compaction goroutine

	writeCh   chan *writeRequest
	flushCh   chan struct{}
	compactCh chan struct{}
	closing   chan struct{}
	wg        sync.WaitGroup

	stats struct {
		flushes, compactions, trivialMoves, writeStalls atomic.Uint64
		bloomSkips, bytesFlushed, bytesCompacted        atomic.Uint64
	}
}

// Open opens or creates the database in dir, recovering any writes found in
// the write-ahead log.
func Open(dir string, opts ...Option) (*DB, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lsmkv: create dir: %w", err)
	}
	db := &DB{
		dir:       dir,
		opts:      o,
		snapshots: make(map[uint64]int),
		writeCh:   make(chan *writeRequest),
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
		closing:   make(chan struct{}),
		picker: compaction.NewPicker(compaction.Options{
			L0Trigger:     o.l0Trigger,
			BaseLevelSize: o.baseLevelSize,
			Multiplier:    o.levelMultiplier,
		}),
	}
	db.cond = sync.NewCond(&db.mu)
	if err := db.recover(); err != nil {
		db.shutdown.Store(true)
		if db.wal != nil {
			_ = db.wal.Close()
		}
		if db.current != nil {
			db.current.unref()
		}
		return nil, err
	}
	db.wg.Add(3)
	go db.writeLoop()
	go db.flushLoop()
	go db.compactLoop()
	db.maybeScheduleCompaction()
	return db, nil
}

// recover loads the manifest, opens every live table, replays the logs that
// were not yet flushed into fresh level-0 tables, and starts a new log.
func (db *DB) recover() error {
	st, _, err := manifest.Load(db.dir)
	if err != nil {
		return err
	}
	tables := make(map[uint64]*table)
	for _, files := range st.Version.Levels {
		for _, f := range files {
			t, err := db.openTable(f)
			if err != nil {
				for _, t := range tables {
					_ = t.r.Close()
				}
				return err
			}
			tables[f.Num] = t
		}
	}
	db.current = newVersion(st.Version, tables)

	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return fmt.Errorf("lsmkv: list dir: %w", err)
	}
	live := st.Version.Files()
	var logs []uint64
	maxNum := uint64(0)
	for _, e := range entries {
		kind, num := parseFileName(e.Name())
		maxNum = max(maxNum, num)
		switch {
		case kind == fileLog && num >= st.LogNum:
			logs = append(logs, num)
		case kind == fileLog, kind == fileTable && !live[num]:
			// A flushed log, or a table orphaned by a crash between
			// writing it and recording it in the manifest.
			if err := os.Remove(db.path(kind, num)); err != nil {
				return fmt.Errorf("lsmkv: remove stale file: %w", err)
			}
		}
	}
	slices.Sort(logs)
	db.nextFileNum.Store(max(st.NextFileNum, maxNum+1, 1))
	db.lastSeq = st.LastSeq

	var edit manifest.Edit
	mem := memtable.New(0)
	flushMem := func() error {
		if mem.Empty() {
			return nil
		}
		t, err := db.writeL0(mem, db.lastSeq)
		if err != nil {
			return err
		}
		edit.Added = append(edit.Added, manifest.LevelFile{Level: 0, Meta: t.meta})
		tables[t.meta.Num] = t
		mem = memtable.New(0)
		return nil
	}
	for _, num := range logs {
		res, err := wal.ReplayFile(logPath(db.dir, num), func(rec []byte) error {
			seq, count, ops, err := decodeRecord(rec)
			if err != nil {
				return err
			}
			i := uint64(0)
			err = forEachOp(ops, count, func(kind base.Kind, key, value []byte) {
				mem.Add(seq+i, kind, key, value)
				i++
			})
			db.lastSeq = max(db.lastSeq, seq+uint64(count)-1)
			return err
		})
		if err != nil {
			return fmt.Errorf("lsmkv: replay log %06d: %w", num, err)
		}
		if res.TornTail {
			db.opts.logger.Warn("discarded torn log tail", "log", num, "validBytes", res.ValidBytes)
		}
		if mem.ApproximateSize() >= db.opts.memtableSize {
			if err := flushMem(); err != nil {
				return err
			}
		}
	}
	if err := flushMem(); err != nil {
		return err
	}

	logNum := db.newFileNum()
	w, err := wal.Create(logPath(db.dir, logNum), wal.Options{Sync: db.opts.sync.toWAL(), Interval: db.opts.syncInterval})
	if err != nil {
		return err
	}
	db.wal = w
	db.mem = &memTable{Memtable: memtable.New(logNum), logNum: logNum}
	db.visibleSeq.Store(db.lastSeq)

	nv, err := st.Version.Apply(edit)
	if err != nil {
		return err
	}
	db.logNum = logNum
	if err := manifest.Store(db.dir, manifest.State{
		NextFileNum: db.nextFileNum.Load(), LogNum: logNum, LastSeq: db.lastSeq, Version: nv,
	}); err != nil {
		return err
	}
	old := db.current
	db.current = newVersion(nv, tables)
	old.unref()
	for _, num := range logs {
		if err := os.Remove(logPath(db.dir, num)); err != nil {
			db.opts.logger.Warn("remove replayed log", "log", num, "err", err)
		}
	}
	if len(logs) > 0 {
		db.opts.logger.Info("recovered", "logs", len(logs), "tables", len(edit.Added), "lastSeq", db.lastSeq)
	}
	return nil
}

func (db *DB) path(kind fileKind, num uint64) string {
	if kind == fileLog {
		return logPath(db.dir, num)
	}
	return tablePath(db.dir, num)
}

func (db *DB) newFileNum() uint64 { return db.nextFileNum.Add(1) - 1 }

func (db *DB) openTable(f *manifest.FileMeta) (*table, error) {
	path := tablePath(db.dir, f.Num)
	r, err := sstable.Open(path)
	if err != nil {
		return nil, fmt.Errorf("lsmkv: open table %06d: %w", f.Num, err)
	}
	return &table{meta: f, r: r, path: path, db: db}, nil
}

// Close flushes the log, stops background work and releases files. Writes
// still in the memtable are recovered from the log on the next Open.
// Iterators and snapshots must be released before Close.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.cond.Broadcast()
	db.mu.Unlock()

	close(db.closing)
	db.wg.Wait()

	err := db.wal.Close()
	db.shutdown.Store(true)
	db.mu.Lock()
	v := db.current
	db.mu.Unlock()
	v.unref()
	return err
}

// readState pins the structures a read needs. The caller must unref v.
func (db *DB) readState() (mem *memTable, imm []*memTable, v *version, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, nil, nil, ErrClosed
	}
	db.current.ref()
	return db.mem, db.imm, db.current, nil
}

// Get returns the value stored under key, or ErrNotFound. The returned slice
// is owned by the caller.
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.get(key, db.visibleSeq.Load())
}

func (db *DB) get(key []byte, seq uint64) ([]byte, error) {
	mem, imm, v, err := db.readState()
	if err != nil {
		return nil, err
	}
	defer v.unref()
	if val, kind, ok := mem.Get(key, seq); ok {
		return liveValue(val, kind, true)
	}
	for i := len(imm) - 1; i >= 0; i-- {
		if val, kind, ok := imm[i].Get(key, seq); ok {
			return liveValue(val, kind, true)
		}
	}
	val, kind, ok, err := v.get(db, key, seq)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	// Table reads allocate a fresh buffer, so no copy is needed.
	return liveValue(val, kind, false)
}

func liveValue(val []byte, kind base.Kind, copyVal bool) ([]byte, error) {
	if kind == base.KindDelete {
		return nil, ErrNotFound
	}
	if copyVal {
		return bytes.Clone(val), nil
	}
	return val, nil
}

// Put stores value under key.
func (db *DB) Put(key, value []byte) error {
	var b Batch
	b.Put(key, value)
	return db.Apply(&b)
}

// Delete removes key. Deleting a missing key is not an error.
func (db *DB) Delete(key []byte) error {
	var b Batch
	b.Delete(key)
	return db.Apply(&b)
}

// Apply commits every operation in b atomically.
func (db *DB) Apply(b *Batch) error {
	if b.Len() == 0 {
		return nil
	}
	if len(b.data)+batchHeaderLen > wal.MaxRecordLen {
		return fmt.Errorf("lsmkv: batch of %d bytes exceeds the %d byte limit", len(b.data), wal.MaxRecordLen)
	}
	return db.submit(&writeRequest{batch: b})
}

// Flush freezes the current memtable and waits until it is written to an
// SSTable.
func (db *DB) Flush() error {
	req := &writeRequest{rotate: true}
	if err := db.submit(req); err != nil {
		return err
	}
	if req.rotated == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for slices.Contains(db.imm, req.rotated) && db.bgErr == nil && !db.closed {
		db.cond.Wait()
	}
	if db.bgErr != nil {
		return db.bgErr
	}
	if slices.Contains(db.imm, req.rotated) {
		return ErrClosed
	}
	return nil
}

// LevelStats describes one level of the tree.
type LevelStats struct {
	Tables int
	Bytes  uint64
}

// Stats is a point-in-time view of engine internals.
type Stats struct {
	Levels             [manifest.NumLevels]LevelStats
	MemtableBytes      int64
	ImmutableMemtables int
	LastSequence       uint64
	Flushes            uint64
	Compactions        uint64
	TrivialMoves       uint64
	WriteStalls        uint64
	BloomSkips         uint64 // table probes avoided by bloom filters
	BytesFlushed       uint64
	BytesCompacted     uint64 // bytes written by compactions
}

// Stats returns current engine statistics.
func (db *DB) Stats() (Stats, error) {
	mem, imm, v, err := db.readState()
	if err != nil {
		return Stats{}, err
	}
	defer v.unref()
	s := Stats{
		MemtableBytes:      mem.ApproximateSize(),
		ImmutableMemtables: len(imm),
		LastSequence:       db.visibleSeq.Load(),
		Flushes:            db.stats.flushes.Load(),
		Compactions:        db.stats.compactions.Load(),
		TrivialMoves:       db.stats.trivialMoves.Load(),
		WriteStalls:        db.stats.writeStalls.Load(),
		BloomSkips:         db.stats.bloomSkips.Load(),
		BytesFlushed:       db.stats.bytesFlushed.Load(),
		BytesCompacted:     db.stats.bytesCompacted.Load(),
	}
	for l := range v.v.Levels {
		s.Levels[l] = LevelStats{Tables: len(v.v.Levels[l]), Bytes: v.v.LevelSize(l)}
	}
	return s, nil
}

func (db *DB) setBGError(err error) {
	if err == nil || errors.Is(err, ErrClosed) {
		return
	}
	db.opts.logger.Error("background error", "err", err)
	db.mu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.cond.Broadcast()
	db.mu.Unlock()
}
