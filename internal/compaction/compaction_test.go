package compaction

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/nikrybak/lsmkv/internal/base"
	"github.com/nikrybak/lsmkv/internal/manifest"
	"github.com/nikrybak/lsmkv/internal/memtable"
)

type op struct {
	key  string
	seq  uint64
	kind base.Kind
}

type recordingSink struct {
	out        []string
	boundaries int
}

func (s *recordingSink) Boundary() error { s.boundaries++; return nil }
func (s *recordingSink) Add(k, _ []byte) error {
	seq, kind := base.Trailer(k)
	tag := "set"
	if kind == base.KindDelete {
		tag = "del"
	}
	s.out = append(s.out, fmt.Sprintf("%s@%d:%s", base.UserKey(k), seq, tag))
	return nil
}

func TestRunDropsShadowedAndTombstones(t *testing.T) {
	ops := []op{
		{"a", 1, base.KindSet}, {"a", 5, base.KindSet}, {"a", 9, base.KindSet},
		{"b", 2, base.KindSet}, {"b", 6, base.KindDelete},
		{"c", 3, base.KindDelete},
		{"d", 8, base.KindDelete}, {"d", 4, base.KindSet},
	}
	tests := []struct {
		name       string
		snapshot   uint64
		bottommost bool
		want       string
	}{
		{"no snapshots, not bottommost", 100, false, "[a@9:set b@6:del c@3:del d@8:del]"},
		{"no snapshots, bottommost", 100, true, "[a@9:set]"},
		{"snapshot at 5 keeps what it can see", 5, true, "[a@9:set a@5:set b@6:del b@2:set d@8:del d@4:set]"},
		{"snapshot at 0 keeps everything", 0, true, "[a@9:set a@5:set a@1:set b@6:del b@2:set c@3:del d@8:del d@4:set]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := memtable.New(1)
			for _, o := range ops {
				m.Add(o.seq, o.kind, []byte(o.key), []byte("v"))
			}
			sink := &recordingSink{}
			st, err := Run(m.NewIterator(), tt.snapshot, tt.bottommost, sink)
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprint(sink.out); got != tt.want {
				t.Fatalf("kept %s, want %s", got, tt.want)
			}
			if st.In != len(ops) || st.Out+st.DroppedShadowed+st.DroppedTombstones != st.In {
				t.Fatalf("inconsistent stats %+v", st)
			}
			keys := map[string]bool{}
			for _, s := range sink.out {
				keys[s[:1]] = true
			}
			if sink.boundaries != len(keys) {
				t.Fatalf("boundaries = %d, want one per kept user key (%d)", sink.boundaries, len(keys))
			}
		})
	}
}

func file(num uint64, lo, hi string, size uint64) *manifest.FileMeta {
	return &manifest.FileMeta{
		Num:      num,
		Size:     size,
		Smallest: base.MakeKey(nil, []byte(lo), 1, base.KindSet),
		Largest:  base.MakeKey(nil, []byte(hi), 1, base.KindSet),
	}
}

func version(t *testing.T, files ...manifest.LevelFile) *manifest.Version {
	t.Helper()
	v, err := (&manifest.Version{}).Apply(manifest.Edit{Added: files})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestPicker(t *testing.T) {
	opts := Options{L0Trigger: 2, BaseLevelSize: 1000, Multiplier: 10}
	tests := []struct {
		name       string
		v          func(t *testing.T) *manifest.Version
		wantNil    bool
		level      int
		in0, in1   int
		bottommost bool
		trivial    bool
	}{
		{"empty", func(t *testing.T) *manifest.Version { return version(t) }, true, 0, 0, 0, false, false},
		{"below trigger", func(t *testing.T) *manifest.Version {
			return version(t, manifest.LevelFile{Level: 0, Meta: file(1, "a", "z", 10)})
		}, true, 0, 0, 0, false, false},
		{"l0 trigger pulls overlapping l1", func(t *testing.T) *manifest.Version {
			return version(t,
				manifest.LevelFile{Level: 0, Meta: file(5, "c", "e", 10)},
				manifest.LevelFile{Level: 0, Meta: file(6, "d", "g", 10)},
				manifest.LevelFile{Level: 1, Meta: file(1, "a", "b", 10)},
				manifest.LevelFile{Level: 1, Meta: file(2, "f", "h", 10)},
				manifest.LevelFile{Level: 2, Meta: file(3, "x", "z", 10)},
			)
		}, false, 0, 2, 1, true, false},
		{"oversized l1 trivially moves", func(t *testing.T) *manifest.Version {
			return version(t,
				manifest.LevelFile{Level: 1, Meta: file(1, "a", "b", 2000)},
				manifest.LevelFile{Level: 2, Meta: file(2, "x", "z", 10)},
			)
		}, false, 1, 1, 0, true, true},
		{"oversized l1 merges with l2", func(t *testing.T) *manifest.Version {
			return version(t,
				manifest.LevelFile{Level: 1, Meta: file(1, "a", "m", 2000)},
				manifest.LevelFile{Level: 2, Meta: file(2, "k", "z", 10)},
				manifest.LevelFile{Level: 3, Meta: file(3, "a", "b", 10)},
			)
		}, false, 1, 1, 1, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewPicker(opts).Pick(tt.v(t))
			if tt.wantNil {
				if c != nil {
					t.Fatalf("picked %v, want nil", c)
				}
				return
			}
			if c == nil {
				t.Fatal("picked nil")
			}
			if c.Level != tt.level || c.OutputLevel != tt.level+1 || len(c.Inputs[0]) != tt.in0 ||
				len(c.Inputs[1]) != tt.in1 || c.Bottommost != tt.bottommost || c.TrivialMove != tt.trivial {
				t.Fatalf("got %v bottommost=%v trivial=%v", c, c.Bottommost, c.TrivialMove)
			}
			e := c.Edit([]*manifest.FileMeta{file(99, "a", "a", 1)})
			if len(e.Deleted) != tt.in0+tt.in1 || len(e.Added) != 1 || e.Added[0].Level != c.OutputLevel {
				t.Fatalf("bad edit %+v", e)
			}
		})
	}
}

func TestPickerRoundRobin(t *testing.T) {
	v := version(t,
		manifest.LevelFile{Level: 1, Meta: file(1, "a", "b", 900)},
		manifest.LevelFile{Level: 1, Meta: file(2, "c", "d", 900)},
		manifest.LevelFile{Level: 1, Meta: file(3, "e", "f", 900)},
	)
	p := NewPicker(Options{BaseLevelSize: 1000})
	var got []uint64
	for i := 0; i < 4; i++ {
		got = append(got, p.Pick(v).Inputs[0][0].Num)
	}
	if !bytes.Equal([]byte(fmt.Sprint(got)), []byte("[1 2 3 1]")) {
		t.Fatalf("picked %v, want round robin [1 2 3 1]", got)
	}
}
