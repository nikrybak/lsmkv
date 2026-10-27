// Package bloom implements the per-table bloom filter used to skip SSTables
// that cannot contain a key.
//
// Encoding: the filter bits followed by one byte holding the number of probes.
// Probes use double hashing (Kirsch and Mitzenmacher) over a single 64-bit
// hash, so one hash computation serves every probe.
package bloom

import "math"

// Builder accumulates key hashes and produces an encoded filter.
type Builder struct {
	bitsPerKey int
	hashes     []uint64
}

// NewBuilder returns a builder sized for bitsPerKey bits per inserted key.
// Ten bits per key gives roughly a one percent false-positive rate.
func NewBuilder(bitsPerKey int) *Builder {
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	return &Builder{bitsPerKey: bitsPerKey}
}

// Add records key in the filter.
func (b *Builder) Add(key []byte) { b.hashes = append(b.hashes, hash(key)) }

// Len returns the number of keys added so far.
func (b *Builder) Len() int { return len(b.hashes) }

// Finish encodes the filter and resets the builder.
func (b *Builder) Finish() []byte {
	k := int(math.Round(float64(b.bitsPerKey) * math.Ln2))
	k = max(1, min(k, 30))

	nbits := max(len(b.hashes)*b.bitsPerKey, 64)
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8

	out := make([]byte, nbytes+1)
	for _, h := range b.hashes {
		h1, h2 := uint32(h), uint32(h>>32)
		for i := 0; i < k; i++ {
			pos := (h1 + uint32(i)*h2) % uint32(nbits)
			out[pos/8] |= 1 << (pos % 8)
		}
	}
	out[nbytes] = byte(k)
	b.hashes = b.hashes[:0]
	return out
}

// MayContain reports whether key may be in the set encoded by filter. A
// malformed filter answers true: losing the optimisation is safe, a false
// negative would lose data.
func MayContain(filter, key []byte) bool {
	if len(filter) < 2 {
		return true
	}
	nbytes := len(filter) - 1
	k := int(filter[nbytes])
	if k < 1 || k > 30 {
		return true
	}
	nbits := uint32(nbytes * 8)
	h := hash(key)
	h1, h2 := uint32(h), uint32(h>>32)
	for i := 0; i < k; i++ {
		pos := (h1 + uint32(i)*h2) % nbits
		if filter[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

func hash(key []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range key {
		h ^= uint64(c)
		h *= prime
	}
	// FNV-1a leaves the high bits poorly mixed for short keys; a final
	// avalanche step keeps the two probe hashes close to independent.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return h
}
