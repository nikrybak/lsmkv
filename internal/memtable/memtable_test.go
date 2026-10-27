package memtable

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"

	"github.com/nikrybak/lsmkv/internal/base"
)

func TestGetVisibility(t *testing.T) {
	m := New(1)
	m.Add(1, base.KindSet, []byte("a"), []byte("a1"))
	m.Add(2, base.KindSet, []byte("b"), []byte("b2"))
	m.Add(3, base.KindSet, []byte("a"), []byte("a3"))
	m.Add(4, base.KindDelete, []byte("b"), nil)

	tests := []struct {
		key       string
		seq       uint64
		wantFound bool
		wantKind  base.Kind
		wantValue string
	}{
		{"a", 0, false, 0, ""},
		{"a", 1, true, base.KindSet, "a1"},
		{"a", 2, true, base.KindSet, "a1"},
		{"a", 3, true, base.KindSet, "a3"},
		{"a", base.MaxSeq, true, base.KindSet, "a3"},
		{"b", 3, true, base.KindSet, "b2"},
		{"b", 4, true, base.KindDelete, ""},
		{"c", base.MaxSeq, false, 0, ""},
		{"", base.MaxSeq, false, 0, ""},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s@%d", tt.key, tt.seq), func(t *testing.T) {
			v, kind, found := m.Get([]byte(tt.key), tt.seq)
			if found != tt.wantFound {
				t.Fatalf("found = %v, want %v", found, tt.wantFound)
			}
			if !found {
				return
			}
			if kind != tt.wantKind || string(v) != tt.wantValue {
				t.Fatalf("got (%q,%d), want (%q,%d)", v, kind, tt.wantValue, tt.wantKind)
			}
		})
	}
}

func TestAddCopiesInput(t *testing.T) {
	m := New(1)
	k, v := []byte("key"), []byte("val")
	m.Add(1, base.KindSet, k, v)
	k[0], v[0] = 'X', 'X'
	got, _, found := m.Get([]byte("key"), 1)
	if !found || string(got) != "val" {
		t.Fatalf("got %q found=%v", got, found)
	}
}

func TestIteratorOrderMatchesSort(t *testing.T) {
	m := New(7)
	rng := rand.New(rand.NewPCG(1, 2))
	var want [][]byte
	for seq := uint64(1); seq <= 2000; seq++ {
		k := []byte(fmt.Sprintf("k%04d", rng.IntN(500)))
		m.Add(seq, base.KindSet, k, k)
		want = append(want, base.MakeKey(nil, k, seq, base.KindSet))
	}
	sort.Slice(want, func(i, j int) bool { return base.Compare(want[i], want[j]) < 0 })

	it := m.NewIterator()
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if !bytes.Equal(it.Key(), want[i]) {
			t.Fatalf("entry %d = %x, want %x", i, it.Key(), want[i])
		}
		i++
	}
	if i != len(want) {
		t.Fatalf("iterated %d entries, want %d", i, len(want))
	}

	it.SeekGE(base.SeekKey([]byte("k0250"), base.MaxSeq))
	if it.Valid() && bytes.Compare(base.UserKey(it.Key()), []byte("k0250")) < 0 {
		t.Fatalf("SeekGE landed before target: %q", base.UserKey(it.Key()))
	}
}

// TestConcurrentReaders checks the single-writer, many-reader contract. It is
// most useful under -race, which CI runs.
func TestConcurrentReaders(t *testing.T) {
	m := New(3)
	const n = 5000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				it := m.NewIterator()
				var prev []byte
				for it.First(); it.Valid(); it.Next() {
					if prev != nil && base.Compare(prev, it.Key()) >= 0 {
						t.Error("iterator out of order")
						return
					}
					prev = it.Key()
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%06d", (i*7919)%n))
		m.Add(uint64(i+1), base.KindSet, k, k)
	}
	close(stop)
	wg.Wait()
	if m.Len() != n {
		t.Fatalf("Len = %d, want %d", m.Len(), n)
	}
}

func BenchmarkAdd(b *testing.B) {
	m := New(1)
	keys := make([][]byte, 1<<16)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%016x", rand.Uint64()))
	}
	val := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Add(uint64(i+1), base.KindSet, keys[i&(len(keys)-1)], val)
	}
}

func BenchmarkGet(b *testing.B) {
	m := New(1)
	const n = 100000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%010d", i))
		m.Add(uint64(i+1), base.KindSet, keys[i], keys[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := m.Get(keys[i%n], base.MaxSeq); !ok {
			b.Fatal("missing")
		}
	}
}

// TestGetNeverReturnsNewerVersion pins a reader at a fixed sequence number
// while the writer keeps adding newer versions of the same key. A search that
// re-read a next pointer after comparing could return one of them.
func TestGetNeverReturnsNewerVersion(t *testing.T) {
	m := New(5)
	key := []byte("hot")
	m.Add(1, base.KindSet, key, []byte("1"))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := uint64(2); seq < 20000; seq++ {
			m.Add(seq, base.KindSet, key, []byte(fmt.Sprint(seq)))
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
		}
		v, _, ok := m.Get(key, 1)
		if !ok || string(v) != "1" {
			t.Fatalf("Get at seq 1 = %q, %v", v, ok)
		}
	}
}
