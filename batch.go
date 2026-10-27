package lsmkv

import (
	"encoding/binary"
	"fmt"

	"github.com/nikrybak/lsmkv/internal/base"
)

// Batch collects writes that are applied atomically: after a crash either
// every operation of a batch is recovered or none is. A Batch is not safe for
// concurrent use and must not be modified while DB.Apply is running.
type Batch struct {
	data  []byte
	count uint32
}

// Put adds a key/value write to the batch. key and value are copied.
func (b *Batch) Put(key, value []byte) {
	b.data = append(b.data, byte(base.KindSet))
	b.data = binary.AppendUvarint(b.data, uint64(len(key)))
	b.data = append(b.data, key...)
	b.data = binary.AppendUvarint(b.data, uint64(len(value)))
	b.data = append(b.data, value...)
	b.count++
}

// Delete adds a deletion to the batch.
func (b *Batch) Delete(key []byte) {
	b.data = append(b.data, byte(base.KindDelete))
	b.data = binary.AppendUvarint(b.data, uint64(len(key)))
	b.data = append(b.data, key...)
	b.count++
}

// Len returns the number of operations in the batch.
func (b *Batch) Len() int { return int(b.count) }

// Reset empties the batch, keeping its buffer.
func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.count = 0
}

// WAL record payload: first sequence u64 | count u32 | operations.
const batchHeaderLen = 12

func (b *Batch) encodeRecord(dst []byte, seq uint64) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, seq)
	dst = binary.LittleEndian.AppendUint32(dst, b.count)
	return append(dst, b.data...)
}

// forEachOp decodes ops and calls fn for each one. It validates everything it
// reads because it also runs over recovered log records.
func forEachOp(ops []byte, count uint32, fn func(kind base.Kind, key, value []byte)) error {
	p := ops
	readBytes := func() ([]byte, bool) {
		n, w := binary.Uvarint(p)
		if w <= 0 || n > uint64(len(p)-w) {
			return nil, false
		}
		out := p[w : w+int(n)]
		p = p[w+int(n):]
		return out, true
	}
	for i := uint32(0); i < count; i++ {
		if len(p) == 0 {
			return fmt.Errorf("batch: truncated at op %d: %w", i, ErrCorrupt)
		}
		kind := base.Kind(p[0])
		p = p[1:]
		key, ok := readBytes()
		if !ok {
			return fmt.Errorf("batch: bad key at op %d: %w", i, ErrCorrupt)
		}
		var value []byte
		switch kind {
		case base.KindSet:
			if value, ok = readBytes(); !ok {
				return fmt.Errorf("batch: bad value at op %d: %w", i, ErrCorrupt)
			}
		case base.KindDelete:
		default:
			return fmt.Errorf("batch: unknown op kind %d: %w", kind, ErrCorrupt)
		}
		fn(kind, key, value)
	}
	if len(p) != 0 {
		return fmt.Errorf("batch: %d trailing bytes: %w", len(p), ErrCorrupt)
	}
	return nil
}

func decodeRecord(rec []byte) (seq uint64, count uint32, ops []byte, err error) {
	if len(rec) < batchHeaderLen {
		return 0, 0, nil, fmt.Errorf("batch: record of %d bytes too short: %w", len(rec), ErrCorrupt)
	}
	return binary.LittleEndian.Uint64(rec), binary.LittleEndian.Uint32(rec[8:]), rec[batchHeaderLen:], nil
}
