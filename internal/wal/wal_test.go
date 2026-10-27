package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nikrybak/lsmkv/internal/base"
)

func writeLog(t *testing.T, opts Options, records [][]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.log")
	w, err := Create(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func replayAll(t *testing.T, data []byte) ([][]byte, ReplayResult, error) {
	t.Helper()
	var got [][]byte
	res, err := Replay(data, func(p []byte) error {
		got = append(got, bytes.Clone(p))
		return nil
	})
	return got, res, err
}

func sampleRecords(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf("record-%03d-%s", i, bytes.Repeat([]byte{'x'}, i)))
	}
	return out
}

func TestRoundTripAllPolicies(t *testing.T) {
	for _, p := range []SyncPolicy{SyncAlways, SyncInterval, SyncNone} {
		t.Run(p.String(), func(t *testing.T) {
			recs := append(sampleRecords(50), []byte{})
			path := writeLog(t, Options{Sync: p, Interval: time.Millisecond}, recs)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			got, res, err := replayAll(t, data)
			if err != nil {
				t.Fatal(err)
			}
			if res.TornTail || res.Records != len(recs) || res.ValidBytes != int64(len(data)) {
				t.Fatalf("unexpected result %+v", res)
			}
			for i := range recs {
				if !bytes.Equal(got[i], recs[i]) {
					t.Fatalf("record %d = %q, want %q", i, got[i], recs[i])
				}
			}
		})
	}
}

func TestCreateRefusesExistingFile(t *testing.T) {
	path := writeLog(t, Options{Sync: SyncNone}, nil)
	if _, err := Create(path, Options{}); err == nil {
		t.Fatal("Create over existing file succeeded")
	}
}

func TestTornTail(t *testing.T) {
	recs := sampleRecords(10)
	path := writeLog(t, Options{Sync: SyncNone}, recs)
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lastLen := HeaderLen + len(recs[9])
	tests := []struct {
		name    string
		data    []byte
		records int
		torn    bool
	}{
		{"intact", full, 10, false},
		{"cut inside header", full[:len(full)-lastLen+3], 9, true},
		{"cut inside payload", full[:len(full)-2], 9, true},
		{"flipped bit in last payload", flip(full, len(full)-1), 9, true},
		{"empty file", nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, res, err := replayAll(t, tt.data)
			if err != nil {
				t.Fatal(err)
			}
			if res.Records != tt.records || res.TornTail != tt.torn || len(got) != tt.records {
				t.Fatalf("got %+v, want records=%d torn=%v", res, tt.records, tt.torn)
			}
		})
	}
}

func TestMidLogCorruptionIsAnError(t *testing.T) {
	recs := sampleRecords(10)
	path := writeLog(t, Options{Sync: SyncNone}, recs)
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the payload of the second record.
	off := HeaderLen + len(recs[0]) + HeaderLen + 1
	got, res, err := replayAll(t, flip(full, off))
	if !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if res.Records != 1 || len(got) != 1 {
		t.Fatalf("records before corruption = %d, want 1", res.Records)
	}
}

func TestCallbackErrorStopsReplay(t *testing.T) {
	path := writeLog(t, Options{Sync: SyncNone}, sampleRecords(5))
	stop := errors.New("stop")
	n := 0
	_, err := ReplayFile(path, func([]byte) error {
		n++
		if n == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || n != 2 {
		t.Fatalf("err=%v n=%d", err, n)
	}
}

func TestAppendAfterCloseFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.log")
	w, err := Create(path, Options{Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("x")); err == nil {
		t.Fatal("append after close succeeded")
	}
}

func flip(data []byte, i int) []byte {
	out := bytes.Clone(data)
	out[i] ^= 0x40
	return out
}

func FuzzWALReplay(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
	path := filepath.Join(f.TempDir(), "seed.log")
	w, err := Create(path, Options{Sync: SyncNone})
	if err != nil {
		f.Fatal(err)
	}
	for _, r := range sampleRecords(3) {
		if err := w.Append(r); err != nil {
			f.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		f.Fatal(err)
	}
	seed, err := os.ReadFile(path)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add(seed[:len(seed)-1])
	f.Fuzz(func(t *testing.T, data []byte) {
		total := 0
		res, err := Replay(data, func(p []byte) error {
			total += HeaderLen + len(p)
			return nil
		})
		if err != nil && !errors.Is(err, base.ErrCorrupt) {
			t.Fatalf("unexpected error type: %v", err)
		}
		if int64(total) != res.ValidBytes || res.ValidBytes > int64(len(data)) {
			t.Fatalf("inconsistent result %+v total=%d len=%d", res, total, len(data))
		}
	})
}
