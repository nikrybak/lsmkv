package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nikrybak/lsmkv/internal/base"
)

func meta(num uint64, lo, hi string) *FileMeta {
	return &FileMeta{
		Num:      num,
		Size:     100 * num,
		Smallest: base.MakeKey(nil, []byte(lo), num, base.KindSet),
		Largest:  base.MakeKey(nil, []byte(hi), num, base.KindSet),
	}
}

func TestStoreLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if _, ok, err := Load(dir); err != nil || ok {
		t.Fatalf("Load on empty dir: ok=%v err=%v", ok, err)
	}
	v, err := (&Version{}).Apply(Edit{Added: []LevelFile{
		{0, meta(3, "a", "z")}, {0, meta(5, "b", "c")},
		{1, meta(2, "m", "p")}, {1, meta(1, "a", "f")},
		{2, meta(4, "a", "z")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := State{NextFileNum: 9, LogNum: 7, LastSeq: 12345, Version: v}
	if err := Store(dir, want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := Load(dir)
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
	}
	if got.Version.Levels[0][0].Num != 5 {
		t.Fatal("level 0 not ordered newest first")
	}
	if got.Version.Levels[1][0].Num != 1 {
		t.Fatal("level 1 not ordered by key")
	}
	if _, err := os.Stat(filepath.Join(dir, tmpName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temp file left behind")
	}
}

func TestApply(t *testing.T) {
	base0 := &Version{}
	v1, err := base0.Apply(Edit{Added: []LevelFile{{0, meta(1, "a", "c")}, {1, meta(2, "d", "f")}}})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		edit    Edit
		wantErr bool
		want    [NumLevels]int
	}{
		{"delete and add", Edit{Deleted: map[uint64]bool{1: true}, Added: []LevelFile{{1, meta(3, "a", "c")}}}, false, [NumLevels]int{0, 2}},
		{"overlap in level 1", Edit{Added: []LevelFile{{1, meta(4, "e", "g")}}}, true, [NumLevels]int{}},
		{"overlap allowed in level 0", Edit{Added: []LevelFile{{0, meta(5, "a", "z")}}}, false, [NumLevels]int{2, 1}},
		{"bad level", Edit{Added: []LevelFile{{NumLevels, meta(6, "a", "b")}}}, true, [NumLevels]int{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nv, err := v1.Apply(tt.edit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			var got [NumLevels]int
			for l := range nv.Levels {
				got[l] = len(nv.Levels[l])
			}
			if got != tt.want {
				t.Fatalf("level counts = %v, want %v", got, tt.want)
			}
			if len(v1.Levels[0]) != 1 || len(v1.Levels[1]) != 1 {
				t.Fatal("Apply mutated the source version")
			}
		})
	}
}

func TestLoadDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	v, _ := (&Version{}).Apply(Edit{Added: []LevelFile{{1, meta(1, "a", "b")}}})
	if err := Store(dir, State{NextFileNum: 2, Version: v}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, FileName)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range good {
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x10
		if err := os.WriteFile(path, bad, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, _, err := Load(dir); !errors.Is(err, base.ErrCorrupt) {
			t.Fatalf("flipping byte %d: err = %v, want ErrCorrupt", i, err)
		}
	}
	if err := os.WriteFile(path, good[:len(good)-1], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(dir); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("truncated: err = %v", err)
	}
}

func TestOverlapping(t *testing.T) {
	v, _ := (&Version{}).Apply(Edit{Added: []LevelFile{
		{1, meta(1, "a", "c")}, {1, meta(2, "e", "g")}, {1, meta(3, "i", "k")},
	}})
	tests := []struct {
		lo, hi string
		want   int
	}{
		{"a", "z", 3}, {"d", "d", 0}, {"c", "e", 2}, {"k", "k", 1}, {"l", "z", 0},
	}
	for _, tt := range tests {
		if got := len(v.Overlapping(1, []byte(tt.lo), []byte(tt.hi))); got != tt.want {
			t.Errorf("Overlapping(%s,%s) = %d, want %d", tt.lo, tt.hi, got, tt.want)
		}
	}
}
