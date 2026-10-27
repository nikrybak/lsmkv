package lsmkv

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nikrybak/lsmkv/internal/base"
)

func TestBatchRoundTrip(t *testing.T) {
	var b Batch
	b.Put([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	b.Put([]byte(""), nil)
	rec := b.encodeRecord(nil, 42)

	seq, count, ops, err := decodeRecord(rec)
	if err != nil || seq != 42 || count != 3 {
		t.Fatalf("decodeRecord = (%d, %d, %v)", seq, count, err)
	}
	var got []string
	err = forEachOp(ops, count, func(kind base.Kind, k, v []byte) {
		got = append(got, fmt.Sprintf("%d:%s=%s", kind, k, v))
	})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[1:a=1 0:b= 1:=]" {
		t.Fatalf("ops = %v", got)
	}
	b.Reset()
	if b.Len() != 0 {
		t.Fatal("Reset left operations behind")
	}
}

func TestBatchDecodeRejectsCorruption(t *testing.T) {
	var b Batch
	b.Put([]byte("key"), []byte("value"))
	good := b.encodeRecord(nil, 1)
	tests := []struct {
		name string
		rec  []byte
	}{
		{"short header", good[:5]},
		{"truncated op", good[:len(good)-2]},
		{"trailing bytes", append(append([]byte(nil), good...), 0)},
		{"unknown kind", func() []byte { r := append([]byte(nil), good...); r[batchHeaderLen] = 9; return r }()},
		{"count too high", func() []byte { r := append([]byte(nil), good...); r[8] = 2; return r }()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, count, ops, err := decodeRecord(tt.rec)
			if err == nil {
				err = forEachOp(ops, count, func(base.Kind, []byte, []byte) {})
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func FuzzBatchDecode(f *testing.F) {
	var b Batch
	b.Put([]byte("k"), []byte("v"))
	b.Delete([]byte("gone"))
	f.Add(b.encodeRecord(nil, 7))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, rec []byte) {
		_, count, ops, err := decodeRecord(rec)
		if err != nil {
			return
		}
		n := uint32(0)
		err = forEachOp(ops, count, func(base.Kind, []byte, []byte) { n++ })
		if err == nil && n != count {
			t.Fatalf("decoded %d ops, header says %d", n, count)
		}
		if err != nil && !errors.Is(err, ErrCorrupt) {
			t.Fatalf("unexpected error type: %v", err)
		}
	})
}
