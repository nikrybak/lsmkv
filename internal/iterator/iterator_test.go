package iterator

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/nikrybak/lsmkv/internal/base"
)

// sliceIter is a reference Iterator over a sorted slice.
type sliceIter struct {
	keys, vals [][]byte
	i          int
	err        error
	failAt     int // index at which Next reports an error; -1 disables
	closed     bool
}

func newSlice(entries ...[2]string) *sliceIter {
	s := &sliceIter{i: len(entries), failAt: -1}
	for _, e := range entries {
		s.keys = append(s.keys, []byte(e[0]))
		s.vals = append(s.vals, []byte(e[1]))
	}
	return s
}

func (s *sliceIter) First() { s.i = 0 }
func (s *sliceIter) SeekGE(k []byte) {
	s.i = sort.Search(len(s.keys), func(i int) bool { return base.Compare(s.keys[i], k) >= 0 })
}
func (s *sliceIter) Next() {
	s.i++
	if s.i == s.failAt {
		s.err = errors.New("boom")
	}
}
func (s *sliceIter) Valid() bool   { return s.err == nil && s.i < len(s.keys) }
func (s *sliceIter) Key() []byte   { return s.keys[s.i] }
func (s *sliceIter) Value() []byte { return s.vals[s.i] }
func (s *sliceIter) Err() error    { return s.err }
func (s *sliceIter) Close() error  { s.closed = true; return nil }

func ik(u string, seq uint64) string { return string(base.MakeKey(nil, []byte(u), seq, base.KindSet)) }

func collect(it Iterator) []string {
	var out []string
	for ; it.Valid(); it.Next() {
		out = append(out, string(base.UserKey(it.Key()))+"="+string(it.Value()))
	}
	return out
}

func TestMergingOrder(t *testing.T) {
	tests := []struct {
		name     string
		children []*sliceIter
		want     []string
	}{
		{"no children", nil, nil},
		{"empty children", []*sliceIter{newSlice(), newSlice()}, nil},
		{"interleaved", []*sliceIter{
			newSlice([2]string{ik("a", 1), "1"}, [2]string{ik("c", 1), "3"}),
			newSlice([2]string{ik("b", 1), "2"}, [2]string{ik("d", 1), "4"}),
		}, []string{"a=1", "b=2", "c=3", "d=4"}},
		{"versions newest first", []*sliceIter{
			newSlice([2]string{ik("k", 2), "old"}),
			newSlice([2]string{ik("k", 9), "new"}),
		}, []string{"k=new", "k=old"}},
		{"equal keys prefer first child", []*sliceIter{
			newSlice([2]string{ik("k", 5), "first"}),
			newSlice([2]string{ik("k", 5), "second"}),
		}, []string{"k=first", "k=second"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cs []Iterator
			for _, c := range tt.children {
				cs = append(cs, c)
			}
			m := NewMerging(cs...)
			m.First()
			got := collect(m)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			for _, c := range tt.children {
				if !c.closed {
					t.Fatal("child not closed")
				}
			}
		})
	}
}

func TestMergingRandomized(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 0))
	var all []string
	var children []Iterator
	seq := uint64(0)
	for c := 0; c < 7; c++ {
		var keys []string
		for i := 0; i < rng.IntN(200); i++ {
			seq++
			keys = append(keys, ik(fmt.Sprintf("%04d", rng.IntN(300)), seq))
		}
		sort.Slice(keys, func(i, j int) bool { return base.Compare([]byte(keys[i]), []byte(keys[j])) < 0 })
		var entries [][2]string
		for _, k := range keys {
			entries = append(entries, [2]string{k, ""})
		}
		all = append(all, keys...)
		children = append(children, newSlice(entries...))
	}
	sort.Slice(all, func(i, j int) bool { return base.Compare([]byte(all[i]), []byte(all[j])) < 0 })

	m := NewMerging(children...)
	m.First()
	i := 0
	for ; m.Valid(); m.Next() {
		if string(m.Key()) != all[i] {
			t.Fatalf("position %d mismatch", i)
		}
		i++
	}
	if i != len(all) {
		t.Fatalf("got %d keys, want %d", i, len(all))
	}

	target := []byte(ik("0150", base.MaxSeq))
	m.SeekGE(target)
	want := sort.Search(len(all), func(i int) bool { return base.Compare([]byte(all[i]), target) >= 0 })
	if want < len(all) && (!m.Valid() || string(m.Key()) != all[want]) {
		t.Fatal("SeekGE mismatch")
	}
}

func TestMergingPropagatesChildError(t *testing.T) {
	bad := newSlice([2]string{ik("a", 1), ""}, [2]string{ik("c", 1), ""})
	bad.failAt = 1
	m := NewMerging(bad, newSlice([2]string{ik("b", 1), ""}))
	m.First()
	n := len(collect(m))
	if m.Err() == nil {
		t.Fatal("child error not reported")
	}
	if n != 1 {
		t.Fatalf("yielded %d entries before error, want 1", n)
	}
}

func TestConcat(t *testing.T) {
	runs := []*sliceIter{
		newSlice([2]string{ik("a", 1), "1"}, [2]string{ik("b", 1), "2"}),
		newSlice(),
		newSlice([2]string{ik("d", 1), "4"}),
		newSlice([2]string{ik("f", 1), "6"}, [2]string{ik("g", 1), "7"}),
	}
	largest := []string{ik("b", 1), ik("c", 1), ik("d", 1), ik("g", 1)}
	opened := 0
	c := NewConcat(len(runs),
		func(i int) []byte { return []byte(largest[i]) },
		func(i int) (Iterator, error) { opened++; return runs[i], nil })

	c.First()
	if got := fmt.Sprint(collect(c)); got != "[a=1 b=2 d=4 f=6 g=7]" {
		t.Fatalf("got %s", got)
	}

	tests := []struct {
		seek string
		want string
	}{
		{"0", "[a=1 b=2 d=4 f=6 g=7]"},
		{"c", "[d=4 f=6 g=7]"},
		{"e", "[f=6 g=7]"},
		{"g", "[g=7]"},
		{"h", "[]"},
	}
	for _, tt := range tests {
		opened = 0
		c.SeekGE([]byte(ik(tt.seek, base.MaxSeq)))
		if got := fmt.Sprint(collect(c)); got != tt.want {
			t.Fatalf("SeekGE(%s) = %s, want %s", tt.seek, got, tt.want)
		}
	}
	opened = 0
	c.SeekGE([]byte(ik("f", base.MaxSeq)))
	if opened != 1 {
		t.Fatalf("SeekGE opened %d runs, want 1", opened)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcatOpenError(t *testing.T) {
	boom := errors.New("open failed")
	c := NewConcat(1, func(int) []byte { return []byte(ik("z", 1)) },
		func(int) (Iterator, error) { return nil, boom })
	c.First()
	if c.Valid() || !errors.Is(c.Err(), boom) {
		t.Fatalf("valid=%v err=%v", c.Valid(), c.Err())
	}
}
