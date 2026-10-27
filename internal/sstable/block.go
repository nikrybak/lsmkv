package sstable

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/nikrybak/lsmkv/internal/base"
)

// Block layout (data and index blocks share it):
//
//	entry*  : shared uvarint | unshared uvarint | valueLen uvarint | key[shared:] | value
//	restart*: uint32 offset of an entry whose shared == 0
//	count   : uint32 number of restart points
//
// Keys are prefix-compressed against the previous key. Every restartInterval
// entries the full key is stored, which lets SeekGE binary-search the restart
// array and scan at most restartInterval entries.

type blockBuilder struct {
	buf             []byte
	restarts        []uint32
	lastKey         []byte
	counter         int
	entries         int
	restartInterval int
}

func newBlockBuilder(restartInterval int) *blockBuilder {
	return &blockBuilder{restartInterval: max(1, restartInterval)}
}

func (b *blockBuilder) add(key, value []byte) {
	shared := 0
	if b.counter < b.restartInterval && b.entries > 0 {
		n := min(len(key), len(b.lastKey))
		for shared < n && key[shared] == b.lastKey[shared] {
			shared++
		}
	} else {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 0
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(key)-shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.lastKey = append(b.lastKey[:0], key...)
	b.counter++
	b.entries++
}

func (b *blockBuilder) estimatedSize() int { return len(b.buf) + 4*len(b.restarts) + 4 }

func (b *blockBuilder) empty() bool { return b.entries == 0 }

// finish returns the encoded block. The slice stays valid until reset.
func (b *blockBuilder) finish() []byte {
	for _, r := range b.restarts {
		b.buf = binary.LittleEndian.AppendUint32(b.buf, r)
	}
	return binary.LittleEndian.AppendUint32(b.buf, uint32(len(b.restarts)))
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:0]
	b.lastKey = b.lastKey[:0]
	b.counter = 0
	b.entries = 0
}

// block is a decoded, validated view over an encoded block.
type block struct {
	data        []byte
	restarts    []uint32
	restartsOff int
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("sstable: "+format+": %w", append(args, base.ErrCorrupt)...)
}

// decodeBlock validates the restart array. Entries are validated lazily by
// the iterator, which never reads outside data.
func decodeBlock(data []byte) (*block, error) {
	if len(data) < 4 {
		return nil, corruptf("block of %d bytes too short", len(data))
	}
	n := int64(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartsOff := int64(len(data)) - 4 - 4*n
	if restartsOff < 0 {
		return nil, corruptf("restart count %d exceeds block size %d", n, len(data))
	}
	if n == 0 && restartsOff != 0 {
		return nil, corruptf("block has entries but no restart points")
	}
	restarts := make([]uint32, n)
	for i := range restarts {
		r := binary.LittleEndian.Uint32(data[restartsOff+int64(4*i):])
		if int64(r) >= restartsOff || (i == 0 && r != 0) || (i > 0 && r <= restarts[i-1]) {
			return nil, corruptf("restart point %d out of order", i)
		}
		restarts[i] = r
	}
	return &block{data: data, restarts: restarts, restartsOff: int(restartsOff)}, nil
}

// blockIter iterates a block. Key returns a buffer owned by the iterator that
// is overwritten by the next positioning call; Value aliases block data.
type blockIter struct {
	b     *block
	key   []byte
	val   []byte
	next  int
	valid bool
	err   error
}

func newBlockIter(b *block) *blockIter { return &blockIter{b: b} }

func (it *blockIter) fail(err error) {
	it.err = err
	it.valid = false
}

func (it *blockIter) decodeAt(off int) {
	if it.err != nil {
		return
	}
	if off >= it.b.restartsOff {
		it.valid = false
		return
	}
	data := it.b.data[:it.b.restartsOff]
	p := off
	var fields [3]uint64
	for i := range fields {
		v, n := binary.Uvarint(data[p:])
		if n <= 0 {
			it.fail(corruptf("bad varint at offset %d", p))
			return
		}
		fields[i] = v
		p += n
	}
	shared, unshared, vlen := fields[0], fields[1], fields[2]
	rest := uint64(len(data) - p)
	if shared > uint64(len(it.key)) || unshared > rest || vlen > rest-unshared {
		it.fail(corruptf("entry at offset %d overflows block", off))
		return
	}
	it.key = append(it.key[:shared], data[p:p+int(unshared)]...)
	p += int(unshared)
	it.val = data[p : p+int(vlen)]
	it.next = p + int(vlen)
	it.valid = true
}

func (it *blockIter) seekToRestart(i int) {
	it.key = it.key[:0]
	it.decodeAt(int(it.b.restarts[i]))
}

// restartKey returns the full key stored at restart point i.
func (it *blockIter) restartKey(i int) ([]byte, error) {
	data := it.b.data[:it.b.restartsOff]
	p := int(it.b.restarts[i])
	var fields [3]uint64
	for j := range fields {
		v, n := binary.Uvarint(data[p:])
		if n <= 0 {
			return nil, corruptf("bad varint at restart %d", i)
		}
		fields[j] = v
		p += n
	}
	if fields[0] != 0 || fields[1] > uint64(len(data)-p) {
		return nil, corruptf("malformed restart entry %d", i)
	}
	return data[p : p+int(fields[1])], nil
}

func (it *blockIter) First() {
	if len(it.b.restarts) == 0 {
		it.valid = false
		return
	}
	it.seekToRestart(0)
}

func (it *blockIter) SeekGE(target []byte) {
	if len(it.b.restarts) == 0 {
		it.valid = false
		return
	}
	idx := sort.Search(len(it.b.restarts), func(i int) bool {
		if it.err != nil {
			return true
		}
		k, err := it.restartKey(i)
		if err != nil {
			it.err = err
			return true
		}
		return base.Compare(k, target) >= 0
	})
	if it.err != nil {
		it.valid = false
		return
	}
	it.seekToRestart(max(0, idx-1))
	for it.valid && base.Compare(it.key, target) < 0 {
		it.Next()
	}
}

func (it *blockIter) Next() {
	if !it.valid {
		return
	}
	it.decodeAt(it.next)
}

func (it *blockIter) Valid() bool   { return it.valid }
func (it *blockIter) Key() []byte   { return it.key }
func (it *blockIter) Value() []byte { return it.val }
func (it *blockIter) Err() error    { return it.err }
