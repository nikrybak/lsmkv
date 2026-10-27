package base

import (
	"bytes"
	"testing"
)

func TestMakeKeyRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		ukey []byte
		seq  uint64
		kind Kind
	}{
		{"empty key", nil, 0, KindSet},
		{"tombstone", []byte("k"), 42, KindDelete},
		{"max seq", []byte("key"), MaxSeq, KindSet},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ik := MakeKey(nil, tt.ukey, tt.seq, tt.kind)
			if !Valid(ik) {
				t.Fatal("key not valid")
			}
			if !bytes.Equal(UserKey(ik), tt.ukey) {
				t.Fatalf("user key = %q, want %q", UserKey(ik), tt.ukey)
			}
			seq, kind := Trailer(ik)
			if seq != tt.seq || kind != tt.kind {
				t.Fatalf("trailer = (%d,%d), want (%d,%d)", seq, kind, tt.seq, tt.kind)
			}
		})
	}
}

func TestValidRejectsUnknownKind(t *testing.T) {
	if Valid([]byte("short")) {
		t.Fatal("short key reported valid")
	}
	if Valid(MakeKey(nil, []byte("k"), 1, Kind(7))) {
		t.Fatal("unknown kind reported valid")
	}
}

func TestCompare(t *testing.T) {
	k := func(u string, seq uint64, kind Kind) []byte { return MakeKey(nil, []byte(u), seq, kind) }
	tests := []struct {
		name string
		a, b []byte
		want int
	}{
		{"user key order", k("a", 1, KindSet), k("b", 1, KindSet), -1},
		{"newer first", k("a", 9, KindSet), k("a", 1, KindSet), -1},
		{"older after", k("a", 1, KindSet), k("a", 9, KindSet), 1},
		{"equal", k("a", 3, KindDelete), k("a", 3, KindDelete), 0},
		{"set before delete at same seq", k("a", 3, KindSet), k("a", 3, KindDelete), -1},
		{"prefix shorter first", k("a", 1, KindSet), k("ab", 9, KindSet), -1},
		{"seek key before entry with same seq", SeekKey([]byte("a"), 5), k("a", 5, KindSet), -1},
		{"seek key after newer entry", SeekKey([]byte("a"), 5), k("a", 6, KindSet), 1},
		{"malformed before valid", []byte("x"), k("a", 1, KindSet), -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Compare(tt.a, tt.b); got != tt.want {
				t.Fatalf("Compare = %d, want %d", got, tt.want)
			}
			if got := Compare(tt.b, tt.a); got != -tt.want {
				t.Fatalf("reverse Compare = %d, want %d", got, -tt.want)
			}
		})
	}
}
