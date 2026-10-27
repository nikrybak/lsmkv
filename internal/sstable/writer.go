package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/nikrybak/lsmkv/internal/base"
	"github.com/nikrybak/lsmkv/internal/bloom"
)

// WriterOptions tunes the table layout.
type WriterOptions struct {
	BlockSize       int // target uncompressed data block size; default 4 KiB
	RestartInterval int // keys between full-key restart points; default 16
	BloomBitsPerKey int // 0 disables the filter
}

func (o *WriterOptions) withDefaults() WriterOptions {
	out := *o
	if out.BlockSize <= 0 {
		out.BlockSize = 4 << 10
	}
	if out.RestartInterval <= 0 {
		out.RestartInterval = 16
	}
	return out
}

// Meta describes a finished table.
type Meta struct {
	Size     uint64
	Entries  int
	Smallest []byte // internal key
	Largest  []byte // internal key
}

// Writer builds a table from internal keys added in strictly increasing order.
type Writer struct {
	path    string
	f       *os.File
	bw      *bufio.Writer
	opts    WriterOptions
	offset  uint64
	data    *blockBuilder
	index   *blockBuilder
	filter  *bloom.Builder
	lastKey []byte
	lastUK  []byte
	meta    Meta
	err     error
}

// Create starts a new table at path.
func Create(path string, opts WriterOptions) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: create: %w", err)
	}
	o := opts.withDefaults()
	w := &Writer{
		path:  path,
		f:     f,
		bw:    bufio.NewWriterSize(f, 256<<10),
		opts:  o,
		data:  newBlockBuilder(o.RestartInterval),
		index: newBlockBuilder(1),
	}
	if o.BloomBitsPerKey > 0 {
		w.filter = bloom.NewBuilder(o.BloomBitsPerKey)
	}
	return w, nil
}

// Add appends an entry. Keys must be valid internal keys in strictly
// increasing order.
func (w *Writer) Add(ikey, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if !base.Valid(ikey) {
		return errors.New("sstable: invalid internal key")
	}
	if w.meta.Entries > 0 && base.Compare(w.lastKey, ikey) >= 0 {
		return fmt.Errorf("sstable: keys out of order: %q after %q", ikey, w.lastKey)
	}
	if w.meta.Entries == 0 {
		w.meta.Smallest = bytes.Clone(ikey)
	}
	if w.filter != nil {
		uk := base.UserKey(ikey)
		if w.meta.Entries == 0 || !bytes.Equal(uk, w.lastUK) {
			w.filter.Add(uk)
			w.lastUK = append(w.lastUK[:0], uk...)
		}
	}
	w.data.add(ikey, value)
	w.lastKey = append(w.lastKey[:0], ikey...)
	w.meta.Entries++
	if w.data.estimatedSize() >= w.opts.BlockSize {
		return w.flushData()
	}
	return nil
}

// EstimatedSize returns the number of bytes the table would occupy if
// finished now, give or take the index and filter.
func (w *Writer) EstimatedSize() uint64 {
	return w.offset + uint64(w.data.estimatedSize())
}

func (w *Writer) flushData() error {
	h, err := w.writeBlock(w.data.finish())
	if err != nil {
		return err
	}
	w.data.reset()
	w.index.add(w.lastKey, h.encode(nil))
	return nil
}

func (w *Writer) writeBlock(b []byte) (handle, error) {
	h := handle{offset: w.offset, length: uint64(len(b))}
	var trailer [blockTrailer]byte
	binary.LittleEndian.PutUint32(trailer[:], crc32.Checksum(b, castagnoli))
	if _, err := w.bw.Write(b); err != nil {
		w.err = fmt.Errorf("sstable: write: %w", err)
		return h, w.err
	}
	if _, err := w.bw.Write(trailer[:]); err != nil {
		w.err = fmt.Errorf("sstable: write: %w", err)
		return h, w.err
	}
	w.offset += uint64(len(b) + blockTrailer)
	return h, nil
}

// Finish writes the filter, index and footer, fsyncs and closes the file.
// The table is durable once Finish returns; making it reachable is the
// caller's job (the manifest).
func (w *Writer) Finish() (Meta, error) {
	if w.err != nil {
		return Meta{}, w.err
	}
	if !w.data.empty() {
		if err := w.flushData(); err != nil {
			return Meta{}, err
		}
	}
	var f footer
	var filterBytes []byte
	if w.filter != nil {
		filterBytes = w.filter.Finish()
	}
	var err error
	if f.filter, err = w.writeBlock(filterBytes); err != nil {
		return Meta{}, err
	}
	if f.index, err = w.writeBlock(w.index.finish()); err != nil {
		return Meta{}, err
	}
	if _, err := w.bw.Write(f.encode()); err != nil {
		return Meta{}, fmt.Errorf("sstable: write footer: %w", err)
	}
	w.offset += footerLen
	if err := w.bw.Flush(); err != nil {
		return Meta{}, fmt.Errorf("sstable: flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return Meta{}, fmt.Errorf("sstable: fsync: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return Meta{}, fmt.Errorf("sstable: close: %w", err)
	}
	w.err = errors.New("sstable: writer finished")
	w.meta.Size = w.offset
	w.meta.Largest = bytes.Clone(w.lastKey)
	return w.meta, nil
}

// Abort discards a partially written table.
func (w *Writer) Abort() {
	w.err = errors.New("sstable: writer aborted")
	_ = w.f.Close()
	_ = os.Remove(w.path)
}
