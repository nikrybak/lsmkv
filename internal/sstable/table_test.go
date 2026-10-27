package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nikrybak/lsmkv/internal/base"
)

type entry struct {
	key  string
	seq  uint64
	kind base.Kind
	val  string
}

func writeTable(t testing.TB, dir string, opts WriterOptions, entries []entry) (string, Meta) {
	t.Helper()
	path := filepath.Join(dir, "000001.sst")
	w, err := Create(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Add(base.MakeKey(nil, []byte(e.key), e.seq, e.kind), []byte(e.val)); err != nil {
			t.Fatal(err)
		}
	}
	m, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return path, m
}

func sequentialEntries(n int) []entry {
	out := make([]entry, n)
	for i := range out {
		out[i] = entry{fmt.Sprintf("key-%06d", i), uint64(i + 1), base.KindSet, fmt.Sprintf("value-%d", i)}
	}
	return out
}

func TestTableGet(t *testing.T) {
	entries := []entry{
		{"a", 5, base.KindSet, "a5"},
		{"a", 2, base.KindSet, "a2"},
		{"b", 4, base.KindDelete, ""},
		{"b", 1, base.KindSet, "b1"},
		{"c", 3, base.KindSet, "c3"},
	}
	for _, opts := range []WriterOptions{{BloomBitsPerKey: 10}, {BlockSize: 1, RestartInterval: 1}} {
		t.Run(fmt.Sprintf("block=%d", opts.BlockSize), func(t *testing.T) {
			path, meta := writeTable(t, t.TempDir(), opts, entries)
			if meta.Entries != len(entries) || string(base.UserKey(meta.Smallest)) != "a" || string(base.UserKey(meta.Largest)) != "c" {
				t.Fatalf("bad meta %+v", meta)
			}
			r, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			tests := []struct {
				key   string
				seq   uint64
				found bool
				kind  base.Kind
				val   string
			}{
				{"a", base.MaxSeq, true, base.KindSet, "a5"},
				{"a", 4, true, base.KindSet, "a2"},
				{"a", 1, false, 0, ""},
				{"b", base.MaxSeq, true, base.KindDelete, ""},
				{"b", 3, true, base.KindSet, "b1"},
				{"c", 3, true, base.KindSet, "c3"},
				{"d", base.MaxSeq, false, 0, ""},
				{"0", base.MaxSeq, false, 0, ""},
			}
			for _, tt := range tests {
				v, kind, found, err := r.Get([]byte(tt.key), tt.seq)
				if err != nil {
					t.Fatal(err)
				}
				if found != tt.found || (found && (kind != tt.kind || string(v) != tt.val)) {
					t.Errorf("Get(%q@%d) = (%q,%d,%v), want (%q,%d,%v)", tt.key, tt.seq, v, kind, found, tt.val, tt.kind, tt.found)
				}
			}
		})
	}
}

func TestTableIterator(t *testing.T) {
	entries := sequentialEntries(5000)
	path, _ := writeTable(t, t.TempDir(), WriterOptions{BlockSize: 512, BloomBitsPerKey: 10}, entries)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if string(base.UserKey(it.Key())) != entries[i].key || string(it.Value()) != entries[i].val {
			t.Fatalf("entry %d mismatch", i)
		}
		i++
	}
	if it.Err() != nil || i != len(entries) {
		t.Fatalf("iterated %d, err %v", i, it.Err())
	}

	it.SeekGE(base.SeekKey([]byte("key-002500x"), base.MaxSeq))
	if !it.Valid() || string(base.UserKey(it.Key())) != "key-002501" {
		t.Fatalf("SeekGE landed on %q", it.Key())
	}
	it.SeekGE(base.SeekKey([]byte("zzz"), base.MaxSeq))
	if it.Valid() {
		t.Fatal("seek past end is valid")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterRejectsOutOfOrderKeys(t *testing.T) {
	w, err := Create(filepath.Join(t.TempDir(), "x.sst"), WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := w.Add(ikey("b", 1), nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(ikey("a", 1), nil); err == nil {
		t.Fatal("out-of-order key accepted")
	}
	if err := w.Add([]byte("short"), nil); err == nil {
		t.Fatal("invalid internal key accepted")
	}
}

func TestBloomSkipsAbsentKeys(t *testing.T) {
	path, _ := writeTable(t, t.TempDir(), WriterOptions{BloomBitsPerKey: 10}, sequentialEntries(1000))
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	skipped := 0
	for i := 0; i < 1000; i++ {
		if !r.MayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			skipped++
		}
	}
	if skipped < 950 {
		t.Fatalf("bloom filter skipped only %d of 1000 absent keys", skipped)
	}
}

func TestOpenDetectsCorruption(t *testing.T) {
	path, _ := writeTable(t, t.TempDir(), WriterOptions{BlockSize: 256, BloomBitsPerKey: 10}, sequentialEntries(200))
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func([]byte) []byte
		onRead bool // corruption only surfaces when the data block is read
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-10] }, false},
		{"bad magic", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }, false},
		{"footer checksum", func(b []byte) []byte { b[len(b)-20] ^= 1; return b }, false},
		{"index block", func(b []byte) []byte { b[len(b)-footerLen-6] ^= 1; return b }, false},
		{"data block", func(b []byte) []byte { b[10] ^= 1; return b }, true},
		{"tiny file", func([]byte) []byte { return []byte("nope") }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "bad.sst")
			if err := os.WriteFile(p, tt.mutate(bytes.Clone(good)), 0o644); err != nil {
				t.Fatal(err)
			}
			r, err := Open(p)
			if !tt.onRead {
				if !errors.Is(err, base.ErrCorrupt) {
					t.Fatalf("Open err = %v, want ErrCorrupt", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			it := r.NewIterator()
			for it.First(); it.Valid(); it.Next() {
			}
			if !errors.Is(it.Err(), base.ErrCorrupt) {
				t.Fatalf("iterator err = %v, want ErrCorrupt", it.Err())
			}
		})
	}
}

type memFile struct{ *bytes.Reader }

func (memFile) Close() error { return nil }

func FuzzTableOpen(f *testing.F) {
	path, _ := writeTable(f, f.TempDir(), WriterOptions{BlockSize: 64, BloomBitsPerKey: 10}, sequentialEntries(20))
	seed, err := os.ReadFile(path)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add(seed[len(seed)/2:])
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := NewReader(memFile{bytes.NewReader(data)}, int64(len(data)))
		if err != nil {
			return
		}
		it := r.NewIterator()
		n := 0
		for it.First(); it.Valid() && n <= len(data); it.Next() {
			n++
		}
		_, _, _, _ = r.Get([]byte("key-000005"), base.MaxSeq)
	})
}

func BenchmarkTableGet(b *testing.B) {
	path, _ := writeTable(b, b.TempDir(), WriterOptions{BloomBitsPerKey: 10}, sequentialEntries(100000))
	r, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("key-%06d", i%100000))
		if _, _, found, err := r.Get(k, base.MaxSeq); err != nil || !found {
			b.Fatal("missing key")
		}
	}
}
