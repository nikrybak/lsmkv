// Package sstable implements the immutable sorted table files.
//
// File layout (format version 1):
//
//	+--------------------+
//	| data block 0       |  each block is followed by a crc32c of its bytes
//	| ...                |
//	| data block N-1     |
//	+--------------------+
//	| filter block       |  bloom filter over user keys
//	+--------------------+
//	| index block        |  one entry per data block:
//	|                    |  last internal key -> block handle
//	+--------------------+
//	| footer (48 bytes)  |
//	+--------------------+
//
// Footer: index offset u64 | index length u64 | filter offset u64 |
// filter length u64 | version u32 | crc32c of the preceding 36 bytes u32 |
// magic u64. All integers are little-endian.
package sstable

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	// FormatVersion is written into every footer.
	FormatVersion = 1
	// Magic identifies an lsmkv table ("LSMKVTB1" read little-endian).
	Magic = 0x3142544b564d534c

	footerLen    = 48
	blockTrailer = 4
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// handle locates a block inside a table. length excludes the checksum.
type handle struct {
	offset uint64
	length uint64
}

func (h handle) encode(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, h.offset)
	return binary.AppendUvarint(dst, h.length)
}

func decodeHandle(src []byte) (handle, error) {
	off, n := binary.Uvarint(src)
	if n <= 0 {
		return handle{}, corruptf("bad block handle")
	}
	length, m := binary.Uvarint(src[n:])
	if m <= 0 || n+m != len(src) {
		return handle{}, corruptf("bad block handle")
	}
	return handle{offset: off, length: length}, nil
}

type footer struct {
	index  handle
	filter handle
}

func (f footer) encode() []byte {
	b := make([]byte, 0, footerLen)
	b = binary.LittleEndian.AppendUint64(b, f.index.offset)
	b = binary.LittleEndian.AppendUint64(b, f.index.length)
	b = binary.LittleEndian.AppendUint64(b, f.filter.offset)
	b = binary.LittleEndian.AppendUint64(b, f.filter.length)
	b = binary.LittleEndian.AppendUint32(b, FormatVersion)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
	return binary.LittleEndian.AppendUint64(b, Magic)
}

func decodeFooter(b []byte) (footer, error) {
	if len(b) != footerLen {
		return footer{}, corruptf("footer has %d bytes", len(b))
	}
	if binary.LittleEndian.Uint64(b[40:]) != Magic {
		return footer{}, corruptf("bad magic number")
	}
	if binary.LittleEndian.Uint32(b[36:]) != crc32.Checksum(b[:36], castagnoli) {
		return footer{}, corruptf("footer checksum mismatch")
	}
	if v := binary.LittleEndian.Uint32(b[32:]); v != FormatVersion {
		return footer{}, corruptf("unsupported format version %d", v)
	}
	return footer{
		index:  handle{binary.LittleEndian.Uint64(b[0:]), binary.LittleEndian.Uint64(b[8:])},
		filter: handle{binary.LittleEndian.Uint64(b[16:]), binary.LittleEndian.Uint64(b[24:])},
	}, nil
}
