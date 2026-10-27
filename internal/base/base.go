// Package base defines the internal key format shared by every layer of the
// storage engine, together with the corruption sentinel error.
//
// An internal key is the user key followed by an 8-byte little-endian trailer
// holding (seq << 8 | kind). Internal keys sort by user key ascending and then
// by trailer descending, so the newest version of a key is met first by any
// forward iterator.
package base

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// ErrCorrupt reports that persisted data failed validation. Every layer wraps
// it so callers can test with errors.Is regardless of where it was detected.
var ErrCorrupt = errors.New("lsmkv: corrupt data")

// Kind distinguishes a value from a tombstone.
type Kind uint8

const (
	// KindDelete marks a tombstone.
	KindDelete Kind = 0
	// KindSet marks a regular value.
	KindSet Kind = 1
	// kindSeek is larger than every stored kind. A seek key built with it
	// sorts before all stored entries that share its user key and sequence.
	kindSeek Kind = 0xff
)

// TrailerLen is the size of the sequence/kind trailer in bytes.
const TrailerLen = 8

// MaxSeq is the largest sequence number representable in a trailer.
const MaxSeq = uint64(1)<<56 - 1

// MakeKey appends the internal key for (ukey, seq, kind) to dst.
func MakeKey(dst, ukey []byte, seq uint64, kind Kind) []byte {
	dst = append(dst, ukey...)
	return binary.LittleEndian.AppendUint64(dst, seq<<8|uint64(kind))
}

// SeekKey returns the internal key that positions an iterator at the newest
// version of ukey whose sequence number is <= seq.
func SeekKey(ukey []byte, seq uint64) []byte {
	return MakeKey(make([]byte, 0, len(ukey)+TrailerLen), ukey, seq, kindSeek)
}

// Valid reports whether ikey is long enough to carry a trailer and its kind
// is one that may be stored.
func Valid(ikey []byte) bool {
	if len(ikey) < TrailerLen {
		return false
	}
	_, k := Trailer(ikey)
	return k == KindSet || k == KindDelete || k == kindSeek
}

// UserKey returns the user key portion of ikey. ikey must be Valid.
func UserKey(ikey []byte) []byte { return ikey[:len(ikey)-TrailerLen] }

// Trailer decodes the sequence number and kind of ikey. ikey must hold at
// least TrailerLen bytes.
func Trailer(ikey []byte) (seq uint64, kind Kind) {
	t := binary.LittleEndian.Uint64(ikey[len(ikey)-TrailerLen:])
	return t >> 8, Kind(t & 0xff)
}

// Compare orders internal keys: user key ascending, trailer descending.
// Keys too short to hold a trailer sort by their raw bytes before valid keys
// so that a malformed key never causes a panic; decoders reject them anyway.
func Compare(a, b []byte) int {
	if len(a) < TrailerLen || len(b) < TrailerLen {
		switch {
		case len(a) >= TrailerLen:
			return 1
		case len(b) >= TrailerLen:
			return -1
		default:
			return bytes.Compare(a, b)
		}
	}
	if c := bytes.Compare(a[:len(a)-TrailerLen], b[:len(b)-TrailerLen]); c != 0 {
		return c
	}
	ta := binary.LittleEndian.Uint64(a[len(a)-TrailerLen:])
	tb := binary.LittleEndian.Uint64(b[len(b)-TrailerLen:])
	switch {
	case ta > tb:
		return -1
	case ta < tb:
		return 1
	}
	return 0
}
