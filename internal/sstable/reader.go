package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/nikrybak/lsmkv/internal/base"
	"github.com/nikrybak/lsmkv/internal/bloom"
)

// ReadAtCloser is the storage a Reader needs. *os.File satisfies it.
type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

// Reader serves point lookups and iterators over one table. The index and
// filter blocks are held in memory; data blocks are read on demand with
// ReadAt, which makes a Reader safe for concurrent use.
type Reader struct {
	r       ReadAtCloser
	dataEnd uint64
	index   *block
	filter  []byte
}

// Open opens the table at path.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: stat: %w", err)
	}
	r, err := NewReader(f, st.Size())
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

// NewReader validates the footer, index and filter of a table of the given
// size. On error the caller still owns r.
func NewReader(r ReadAtCloser, size int64) (*Reader, error) {
	if size < footerLen {
		return nil, corruptf("file of %d bytes too small for a table", size)
	}
	buf := make([]byte, footerLen)
	if _, err := r.ReadAt(buf, size-footerLen); err != nil {
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	ft, err := decodeFooter(buf)
	if err != nil {
		return nil, err
	}
	t := &Reader{r: r, dataEnd: uint64(size - footerLen)}
	indexData, err := t.readRaw(ft.index)
	if err != nil {
		return nil, fmt.Errorf("index block: %w", err)
	}
	if t.index, err = decodeBlock(indexData); err != nil {
		return nil, fmt.Errorf("index block: %w", err)
	}
	if t.filter, err = t.readRaw(ft.filter); err != nil {
		return nil, fmt.Errorf("filter block: %w", err)
	}
	return t, nil
}

func (t *Reader) readRaw(h handle) ([]byte, error) {
	if h.length > t.dataEnd || h.offset > t.dataEnd-h.length || t.dataEnd-h.length-h.offset < blockTrailer {
		return nil, corruptf("block handle %+v outside table", h)
	}
	buf := make([]byte, h.length+blockTrailer)
	if _, err := t.r.ReadAt(buf, int64(h.offset)); err != nil {
		return nil, fmt.Errorf("sstable: read block: %w", err)
	}
	data := buf[:h.length]
	if binary.LittleEndian.Uint32(buf[h.length:]) != crc32.Checksum(data, castagnoli) {
		return nil, corruptf("block checksum mismatch at offset %d", h.offset)
	}
	return data, nil
}

func (t *Reader) readBlock(encodedHandle []byte) (*block, error) {
	h, err := decodeHandle(encodedHandle)
	if err != nil {
		return nil, err
	}
	data, err := t.readRaw(h)
	if err != nil {
		return nil, err
	}
	return decodeBlock(data)
}

// MayContain consults the bloom filter for a user key.
func (t *Reader) MayContain(ukey []byte) bool {
	return len(t.filter) == 0 || bloom.MayContain(t.filter, ukey)
}

// Get returns the newest version of ukey visible at seq. The returned value
// is owned by the caller. A tombstone is returned as found with KindDelete.
func (t *Reader) Get(ukey []byte, seq uint64) (value []byte, kind base.Kind, found bool, err error) {
	if !t.MayContain(ukey) {
		return nil, 0, false, nil
	}
	it := t.NewIterator()
	defer it.Close()
	it.SeekGE(base.SeekKey(ukey, seq))
	if !it.Valid() {
		return nil, 0, false, it.Err()
	}
	if !bytes.Equal(base.UserKey(it.Key()), ukey) {
		return nil, 0, false, nil
	}
	_, kind = base.Trailer(it.Key())
	// Data blocks are freshly allocated per read and never reused, so the
	// value can be returned without a copy.
	return it.Value(), kind, true, nil
}

// Close releases the underlying file.
func (t *Reader) Close() error { return t.r.Close() }

// NewIterator returns a two-level iterator: the index block selects a data
// block, the data block iterator walks entries.
func (t *Reader) NewIterator() *Iterator {
	return &Iterator{t: t, index: newBlockIter(t.index)}
}

// Iterator walks a table in internal-key order. Key and Value are valid until
// the next positioning call.
type Iterator struct {
	t     *Reader
	index *blockIter
	data  *blockIter
	err   error
}

var errClosed = errors.New("sstable: iterator closed")

func (it *Iterator) loadBlock() bool {
	it.data = nil
	if !it.index.Valid() {
		if err := it.index.Err(); err != nil {
			it.err = err
		}
		return false
	}
	b, err := it.t.readBlock(it.index.Value())
	if err != nil {
		it.err = err
		return false
	}
	it.data = newBlockIter(b)
	return true
}

// skipEmpty moves forward over exhausted data blocks.
func (it *Iterator) skipEmpty() {
	for it.err == nil && it.data != nil && !it.data.Valid() {
		if err := it.data.Err(); err != nil {
			it.err = err
			it.data = nil
			return
		}
		it.index.Next()
		if !it.loadBlock() {
			return
		}
		it.data.First()
	}
	if it.err == nil && it.data != nil && !base.Valid(it.data.Key()) {
		it.err = corruptf("invalid internal key in data block")
	}
}

// First positions the iterator at the smallest key.
func (it *Iterator) First() {
	if it.err != nil {
		return
	}
	it.index.First()
	if it.loadBlock() {
		it.data.First()
		it.skipEmpty()
	}
}

// SeekGE positions the iterator at the first key >= target.
func (it *Iterator) SeekGE(target []byte) {
	if it.err != nil {
		return
	}
	it.index.SeekGE(target)
	if it.loadBlock() {
		it.data.SeekGE(target)
		it.skipEmpty()
	}
}

// Next advances the iterator.
func (it *Iterator) Next() {
	if !it.Valid() {
		return
	}
	it.data.Next()
	it.skipEmpty()
}

// Valid reports whether the iterator is positioned at an entry.
func (it *Iterator) Valid() bool { return it.err == nil && it.data != nil && it.data.Valid() }

// Key returns the current internal key.
func (it *Iterator) Key() []byte { return it.data.Key() }

// Value returns the current value.
func (it *Iterator) Value() []byte { return it.data.Value() }

// Err returns the first error encountered.
func (it *Iterator) Err() error {
	if it.err == errClosed {
		return nil
	}
	return it.err
}

// Close releases the iterator. The table itself stays open.
func (it *Iterator) Close() error {
	it.data = nil
	it.err = errClosed
	return nil
}
