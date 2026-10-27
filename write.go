package lsmkv

import (
	"github.com/nikrybak/lsmkv/internal/base"
	"github.com/nikrybak/lsmkv/internal/memtable"
	"github.com/nikrybak/lsmkv/internal/wal"
)

const (
	maxGroupRequests = 256
	maxGroupBytes    = 4 << 20
)

type writeRequest struct {
	batch   *Batch
	rotate  bool      // freeze the memtable (Flush)
	rotated *memTable // set by the writer when rotate froze a memtable
	done    chan error
}

// submit hands req to the writer goroutine and waits for the outcome. The
// channel is unbuffered, so a successful send means the writer owns req.
func (db *DB) submit(req *writeRequest) error {
	req.done = make(chan error, 1)
	select {
	case db.writeCh <- req:
	case <-db.closing:
		return ErrClosed
	}
	return <-req.done
}

// writeLoop is the single writer. Each iteration takes one request, then
// greedily drains whatever else is already queued so that all of them share
// one log commit (and one fsync under SyncAlways).
func (db *DB) writeLoop() {
	defer db.wg.Done()
	group := make([]*writeRequest, 0, maxGroupRequests)
	for {
		var first *writeRequest
		select {
		case first = <-db.writeCh:
		case <-db.closing:
			return
		}
		group = append(group[:0], first)
		size := 0
		if first.batch != nil {
			size = len(first.batch.data)
		}
	drain:
		for !first.rotate && len(group) < maxGroupRequests && size < maxGroupBytes {
			select {
			case r := <-db.writeCh:
				group = append(group, r)
				if r.rotate {
					break drain
				}
				size += len(r.batch.data)
			default:
				break drain
			}
		}
		err := db.commitGroup(group)
		for _, r := range group {
			r.done <- err
		}
	}
}

func (db *DB) commitGroup(group []*writeRequest) error {
	forceRotate := false
	for _, r := range group {
		forceRotate = forceRotate || r.rotate
	}
	if err := db.makeRoom(false); err != nil {
		return err
	}

	seq := db.lastSeq + 1
	var rec []byte
	appended := false
	for _, r := range group {
		if r.batch == nil {
			continue
		}
		rec = r.batch.encodeRecord(rec[:0], seq)
		if err := db.wal.Append(rec); err != nil {
			return err
		}
		seq += uint64(r.batch.count)
		appended = true
	}
	if appended {
		if err := db.wal.Commit(); err != nil {
			return err
		}
		seq = db.lastSeq + 1
		for _, r := range group {
			if r.batch == nil {
				continue
			}
			err := forEachOp(r.batch.data, r.batch.count, func(kind base.Kind, key, value []byte) {
				db.mem.Add(seq, kind, key, value)
				seq++
			})
			if err != nil {
				return err
			}
		}
		db.lastSeq = seq - 1
		// Publishing after the whole group is applied keeps every batch
		// atomic for readers: none can observe half of it.
		db.visibleSeq.Store(db.lastSeq)
	}

	if forceRotate && !db.mem.Empty() {
		frozen := db.mem
		if err := db.makeRoom(true); err != nil {
			return err
		}
		for _, r := range group {
			if r.rotate {
				r.rotated = frozen
			}
		}
	}
	return nil
}

// makeRoom freezes the memtable when it is full (or force is set) and starts
// a new log. It stalls while too many memtables await flushing or level 0
// has too many tables: backpressure is preferable to unbounded memory and
// read amplification.
func (db *DB) makeRoom(force bool) error {
	if !force && db.mem.ApproximateSize() < db.opts.memtableSize {
		return nil
	}
	db.mu.Lock()
	stalled := false
	for db.bgErr == nil && !db.closed &&
		(len(db.imm) >= db.opts.maxImmutable || len(db.current.v.Levels[0]) >= db.opts.l0StopWrites) {
		if !stalled {
			stalled = true
			db.stats.writeStalls.Add(1)
			db.opts.logger.Warn("write stall", "immutable", len(db.imm), "l0", len(db.current.v.Levels[0]))
		}
		db.scheduleFlush()
		db.cond.Wait()
	}
	err := db.bgErr
	if db.closed {
		err = ErrClosed
	}
	db.mu.Unlock()
	if err != nil {
		return err
	}

	num := db.newFileNum()
	w, err := wal.Create(logPath(db.dir, num), wal.Options{Sync: db.opts.sync.toWAL(), Interval: db.opts.syncInterval})
	if err != nil {
		return err
	}
	// The old log must be durable before it is the only copy of the frozen
	// memtable that survives a crash.
	if err := db.wal.Close(); err != nil {
		_ = w.Close()
		return err
	}
	db.wal = w

	db.mu.Lock()
	db.imm = append(db.imm[:len(db.imm):len(db.imm)], db.mem)
	db.mem = &memTable{Memtable: memtable.New(num), logNum: num}
	db.scheduleFlush()
	db.mu.Unlock()
	return nil
}

func (db *DB) scheduleFlush() {
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
}

func (db *DB) maybeScheduleCompaction() {
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}
