// Package wal implements the write-ahead log.
//
// A log file is a sequence of records:
//
//	+-----------+-----------+-----------------+
//	| crc32c u32| length u32| payload (length)|
//	+-----------+-----------+-----------------+
//
// Both integers are little-endian; the checksum covers the length field and
// the payload, so a torn length is detected as well as a torn payload. The
// package treats payloads as opaque; the engine stores one encoded write batch
// per record, which makes every batch atomic on recovery.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"

	"github.com/nikrybak/lsmkv/internal/base"
)

// HeaderLen is the size of the per-record header.
const HeaderLen = 8

// MaxRecordLen bounds a single record so that a corrupt length field cannot
// make recovery allocate unbounded memory.
const MaxRecordLen = 64 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// SyncPolicy controls when appended records are forced to stable storage.
type SyncPolicy int

const (
	// SyncAlways fsyncs on every Commit: an acknowledged write survives power
	// loss.
	SyncAlways SyncPolicy = iota
	// SyncInterval fsyncs from a background ticker: a power loss may drop the
	// last interval of acknowledged writes, a process crash drops nothing.
	SyncInterval
	// SyncNone never fsyncs explicitly and leaves durability to the OS.
	SyncNone
)

// String implements fmt.Stringer.
func (p SyncPolicy) String() string {
	switch p {
	case SyncAlways:
		return "always"
	case SyncInterval:
		return "interval"
	case SyncNone:
		return "none"
	}
	return fmt.Sprintf("SyncPolicy(%d)", int(p))
}

// Options configures a Writer.
type Options struct {
	Sync     SyncPolicy
	Interval time.Duration // used by SyncInterval; defaults to 100ms
}

// Writer appends records to a single log file. It is safe for concurrent use,
// although the engine drives it from one goroutine plus the sync ticker.
type Writer struct {
	mu      sync.Mutex
	f       *os.File
	bw      *bufio.Writer
	opts    Options
	size    int64
	dirty   bool
	err     error // sticky: once a write or sync fails the log is unusable
	stop    chan struct{}
	stopped chan struct{}
}

// Create creates a new log file at path. It fails if the file exists, which
// guards against reusing a file number.
func Create(path string, opts Options) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: create: %w", err)
	}
	w := &Writer{f: f, bw: bufio.NewWriterSize(f, 64<<10), opts: opts}
	if opts.Sync == SyncInterval {
		if w.opts.Interval <= 0 {
			w.opts.Interval = 100 * time.Millisecond
		}
		w.stop = make(chan struct{})
		w.stopped = make(chan struct{})
		go w.syncLoop()
	}
	return w, nil
}

func (w *Writer) syncLoop() {
	defer close(w.stopped)
	t := time.NewTicker(w.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-t.C:
			w.mu.Lock()
			if w.dirty && w.err == nil {
				w.err = w.syncLocked()
			}
			w.mu.Unlock()
		}
	}
}

// Append buffers one record. It is not durable until Commit returns.
func (w *Writer) Append(payload []byte) error {
	if len(payload) > MaxRecordLen {
		return fmt.Errorf("wal: record of %d bytes exceeds limit %d", len(payload), MaxRecordLen)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	var hdr [HeaderLen]byte
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(payload)))
	crc := crc32.Update(0, castagnoli, hdr[4:])
	crc = crc32.Update(crc, castagnoli, payload)
	binary.LittleEndian.PutUint32(hdr[:4], crc)
	if _, err := w.bw.Write(hdr[:]); err != nil {
		w.err = fmt.Errorf("wal: append: %w", err)
		return w.err
	}
	if _, err := w.bw.Write(payload); err != nil {
		w.err = fmt.Errorf("wal: append: %w", err)
		return w.err
	}
	w.size += int64(HeaderLen + len(payload))
	w.dirty = true
	return nil
}

// Commit hands buffered records to the OS and, under SyncAlways, waits for
// them to reach stable storage. Callers append a whole group of writes and
// commit once, which is what makes group commit cheap.
func (w *Writer) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if err := w.bw.Flush(); err != nil {
		w.err = fmt.Errorf("wal: flush: %w", err)
		return w.err
	}
	if w.opts.Sync == SyncAlways {
		w.err = w.syncLocked()
	}
	return w.err
}

// Sync flushes and fsyncs regardless of policy.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.err = w.syncLocked()
	return w.err
}

func (w *Writer) syncLocked() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	w.dirty = false
	return nil
}

// Size returns the number of bytes appended so far.
func (w *Writer) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Close syncs outstanding records and closes the file.
func (w *Writer) Close() error {
	if w.stop != nil {
		close(w.stop)
		<-w.stopped
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.err
	if err == nil {
		err = w.syncLocked()
	}
	if cerr := w.f.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("wal: close: %w", cerr)
	}
	if w.err == nil {
		w.err = errors.New("wal: closed")
	}
	return err
}

// ReplayResult describes what recovery found in a log.
type ReplayResult struct {
	Records    int
	ValidBytes int64 // length of the intact prefix
	TornTail   bool  // a partially written final record was discarded
}

// Replay decodes every record in data and calls fn with each payload. The
// payload slice aliases data.
//
// A damaged final record is the expected footprint of a crash in the middle of
// a write, so it is reported via TornTail and not as an error. Damage that is
// followed by further data cannot come from a torn append and is reported as
// base.ErrCorrupt.
func Replay(data []byte, fn func(payload []byte) error) (ReplayResult, error) {
	var res ReplayResult
	off := 0
	for off < len(data) {
		rest := data[off:]
		if len(rest) < HeaderLen {
			res.TornTail = true
			break
		}
		n := int(binary.LittleEndian.Uint32(rest[4:8]))
		if n > MaxRecordLen || HeaderLen+n > len(rest) {
			// The record claims to run past the end of the file. Either the
			// tail was torn or the length is garbage; both leave no
			// trustworthy data after this point.
			res.TornTail = true
			break
		}
		want := binary.LittleEndian.Uint32(rest[:4])
		got := crc32.Checksum(rest[4:HeaderLen+n], castagnoli)
		if want != got {
			if HeaderLen+n == len(rest) {
				res.TornTail = true
				break
			}
			return res, fmt.Errorf("wal: checksum mismatch at offset %d: %w", off, base.ErrCorrupt)
		}
		if err := fn(rest[HeaderLen : HeaderLen+n]); err != nil {
			return res, err
		}
		off += HeaderLen + n
		res.Records++
		res.ValidBytes = int64(off)
	}
	return res, nil
}

// ReplayFile reads the log at path and replays it with Replay. Log files are
// bounded by the memtable size, so reading them whole is cheap and keeps the
// tail-detection logic simple.
func ReplayFile(path string, fn func(payload []byte) error) (ReplayResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("wal: read: %w", err)
	}
	return Replay(data, fn)
}
