package lsmkv

import (
	"io"
	"log/slog"
	"time"

	"github.com/nikrybak/lsmkv/internal/wal"
)

// SyncPolicy controls when the write-ahead log is forced to stable storage.
type SyncPolicy int

const (
	// SyncAlways fsyncs the log before a write is acknowledged. Concurrent
	// writers share one fsync through group commit.
	SyncAlways SyncPolicy = iota
	// SyncInterval fsyncs the log periodically (see WithSyncInterval). A
	// power failure can lose up to one interval of acknowledged writes; a
	// process crash loses none.
	SyncInterval
	// SyncNone leaves flushing to the operating system.
	SyncNone
)

func (p SyncPolicy) toWAL() wal.SyncPolicy {
	switch p {
	case SyncInterval:
		return wal.SyncInterval
	case SyncNone:
		return wal.SyncNone
	}
	return wal.SyncAlways
}

type options struct {
	memtableSize    int64
	maxImmutable    int
	sync            SyncPolicy
	syncInterval    time.Duration
	blockSize       int
	bloomBitsPerKey int
	l0Trigger       int
	l0StopWrites    int
	baseLevelSize   uint64
	levelMultiplier uint64
	targetFileSize  uint64
	logger          *slog.Logger
}

func defaultOptions() options {
	return options{
		memtableSize:    4 << 20,
		maxImmutable:    2,
		sync:            SyncAlways,
		syncInterval:    100 * time.Millisecond,
		blockSize:       4 << 10,
		bloomBitsPerKey: 10,
		l0Trigger:       4,
		l0StopWrites:    12,
		baseLevelSize:   10 << 20,
		levelMultiplier: 10,
		targetFileSize:  2 << 20,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// Option configures a DB at Open.
type Option func(*options)

// WithMemtableSize sets the memtable size, in bytes, at which it is frozen
// and flushed to level 0. Default 4 MiB.
func WithMemtableSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.memtableSize = int64(n)
		}
	}
}

// WithMaxImmutableMemtables bounds how many frozen memtables may wait for
// flushing before writes stall. Default 2.
func WithMaxImmutableMemtables(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.maxImmutable = n
		}
	}
}

// WithSyncPolicy sets the write-ahead log durability policy. Default
// SyncAlways.
func WithSyncPolicy(p SyncPolicy) Option {
	return func(o *options) { o.sync = p }
}

// WithSyncInterval selects SyncInterval with the given period.
func WithSyncInterval(d time.Duration) Option {
	return func(o *options) {
		o.sync = SyncInterval
		if d > 0 {
			o.syncInterval = d
		}
	}
}

// WithBlockSize sets the target SSTable data block size. Default 4 KiB.
func WithBlockSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.blockSize = n
		}
	}
}

// WithBloomBitsPerKey sets the bloom filter density; 0 disables filters.
// Default 10 (about 1% false positives).
func WithBloomBitsPerKey(n int) Option {
	return func(o *options) {
		if n >= 0 {
			o.bloomBitsPerKey = n
		}
	}
}

// WithL0CompactionTrigger sets the number of level-0 tables that triggers a
// compaction into level 1. Default 4.
func WithL0CompactionTrigger(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.l0Trigger = n
		}
	}
}

// WithL0StopWritesTrigger sets the number of level-0 tables at which writes
// stall until compaction catches up. Default 12.
func WithL0StopWritesTrigger(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.l0StopWrites = n
		}
	}
}

// WithLevelSizes sets the level-1 size budget and the growth factor between
// levels. Defaults 10 MiB and 10.
func WithLevelSizes(base uint64, multiplier uint64) Option {
	return func(o *options) {
		if base > 0 {
			o.baseLevelSize = base
		}
		if multiplier >= 2 {
			o.levelMultiplier = multiplier
		}
	}
}

// WithTargetFileSize sets the size at which compaction output is split into
// a new table. Default 2 MiB.
func WithTargetFileSize(n uint64) Option {
	return func(o *options) {
		if n > 0 {
			o.targetFileSize = n
		}
	}
}

// WithLogger routes background events (flushes, compactions, recovery
// notes) to l. By default they are discarded.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}
