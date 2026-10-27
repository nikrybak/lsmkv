package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/nikrybak/lsmkv/internal/base"
)

func ikey(u string, seq uint64) []byte { return base.MakeKey(nil, []byte(u), seq, base.KindSet) }

func buildBlock(restart int, keys []string) []byte {
	b := newBlockBuilder(restart)
	for i, k := range keys {
		b.add(ikey(k, uint64(len(keys)-i)), []byte("v-"+k))
	}
	return bytes.Clone(b.finish())
}

func TestBlockRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		restart int
		keys    []string
	}{
		{"empty", 16, nil},
		{"single", 16, []string{"a"}},
		{"restart every key", 1, []string{"a", "ab", "abc", "b"}},
		{"shared prefixes", 4, []string{"user:0001", "user:0002", "user:0010", "user:0100", "user:1000", "zz"}},
		{"many", 16, func() []string {
			var out []string
			for i := 0; i < 500; i++ {
				out = append(out, fmt.Sprintf("key-%05d", i))
			}
			return out
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blk, err := decodeBlock(buildBlock(tt.restart, tt.keys))
			if err != nil {
				t.Fatal(err)
			}
			it := newBlockIter(blk)
			i := 0
			for it.First(); it.Valid(); it.Next() {
				if string(base.UserKey(it.Key())) != tt.keys[i] || string(it.Value()) != "v-"+tt.keys[i] {
					t.Fatalf("entry %d = %q/%q", i, it.Key(), it.Value())
				}
				i++
			}
			if it.Err() != nil || i != len(tt.keys) {
				t.Fatalf("iterated %d of %d, err=%v", i, len(tt.keys), it.Err())
			}
			for j, k := range tt.keys {
				it.SeekGE(base.SeekKey([]byte(k), base.MaxSeq))
				if !it.Valid() || string(base.UserKey(it.Key())) != k {
					t.Fatalf("SeekGE(%q) failed at %d", k, j)
				}
			}
			it.SeekGE(base.SeekKey([]byte("\xff\xff"), base.MaxSeq))
			if it.Valid() {
				t.Fatal("SeekGE past the end is valid")
			}
		})
	}
}

func TestBlockSeekBetweenKeys(t *testing.T) {
	blk, err := decodeBlock(buildBlock(2, []string{"b", "d", "f", "h"}))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct{ target, want string }{
		{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"h", "h"},
	}
	it := newBlockIter(blk)
	for _, tt := range tests {
		it.SeekGE(base.SeekKey([]byte(tt.target), base.MaxSeq))
		if !it.Valid() || string(base.UserKey(it.Key())) != tt.want {
			t.Fatalf("SeekGE(%q) = %q, want %q", tt.target, it.Key(), tt.want)
		}
	}
}

func TestDecodeBlockRejectsCorruption(t *testing.T) {
	good := buildBlock(2, []string{"a", "b", "c"})
	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{1, 2}},
		{"restart count too large", binary.LittleEndian.AppendUint32(nil, 1000)},
		{"entries without restarts", append(bytes.Clone(good[:len(good)-4]), 0, 0, 0, 0)},
		{"restart out of range", func() []byte {
			b := bytes.Clone(good)
			binary.LittleEndian.PutUint32(b[len(b)-8:], 1<<20)
			return b
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeBlock(tt.data); !errors.Is(err, base.ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestBlockIterReportsBadEntry(t *testing.T) {
	// One restart at 0 followed by an entry whose length runs past the block.
	data := []byte{0, 200, 0}
	data = binary.LittleEndian.AppendUint32(data, 0)
	data = binary.LittleEndian.AppendUint32(data, 1)
	blk, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	it := newBlockIter(blk)
	it.First()
	if it.Valid() || !errors.Is(it.Err(), base.ErrCorrupt) {
		t.Fatalf("valid=%v err=%v", it.Valid(), it.Err())
	}
}

// FuzzBlockDecode feeds arbitrary bytes to the block decoder and iterator.
// The decoder must reject or safely iterate any input, and every block that
// decodes must yield keys in the order it stores them without panicking.
func FuzzBlockDecode(f *testing.F) {
	f.Add(buildBlock(16, nil))
	f.Add(buildBlock(1, []string{"a", "b"}))
	f.Add(buildBlock(3, []string{"apple", "apricot", "banana", "blueberry", "cherry"}))
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		blk, err := decodeBlock(data)
		if err != nil {
			if !errors.Is(err, base.ErrCorrupt) {
				t.Fatalf("non-corruption error: %v", err)
			}
			return
		}
		it := newBlockIter(blk)
		n := 0
		var first []byte
		for it.First(); it.Valid(); it.Next() {
			if n == 0 {
				first = bytes.Clone(it.Key())
			}
			if n++; n > len(data) {
				t.Fatal("iterator yielded more entries than bytes")
			}
		}
		if err := it.Err(); err != nil && !errors.Is(err, base.ErrCorrupt) {
			t.Fatalf("non-corruption error: %v", err)
		}
		if first != nil {
			it.SeekGE(first)
		}
		it.SeekGE([]byte("probe-key-with-trailer"))
	})
}
