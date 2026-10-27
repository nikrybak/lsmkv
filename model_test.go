package lsmkv

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
)

// TestRandomizedAgainstModel drives the engine with a random mix of puts,
// deletes, batches, scans, snapshots, flushes and reopen cycles, and checks
// every observation against a plain map. Small memtables and level budgets
// make flushes and multi-level compactions happen constantly.
func TestRandomizedAgainstModel(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	ops := 6000
	if testing.Short() {
		seeds, ops = seeds[:1], 2000
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runModel(t, seed, ops)
		})
	}
}

type modelSnapshot struct {
	snap  *Snapshot
	state map[string]string
}

func runModel(t *testing.T, seed uint64, ops int) {
	rng := rand.New(rand.NewPCG(seed, seed*7919))
	dir := t.TempDir()
	opts := smallOpts(WithMemtableSize(8 << 10))
	db := openDB(t, dir, opts...)
	defer func() { db.Close() }()

	model := map[string]string{}
	var snaps []modelSnapshot
	key := func() string { return fmt.Sprintf("key-%04d", rng.IntN(800)) }
	value := func() string { return fmt.Sprintf("v%d-%s", rng.Uint32(), bytes.Repeat([]byte("x"), rng.IntN(64))) }

	verifyGet := func(get func([]byte) ([]byte, error), state map[string]string, k string) {
		t.Helper()
		got, err := get([]byte(k))
		want, ok := state[k]
		switch {
		case ok && (err != nil || string(got) != want):
			t.Fatalf("Get(%s) = %q, %v; want %q", k, got, err, want)
		case !ok && !errors.Is(err, ErrNotFound):
			t.Fatalf("Get(%s) = %q, %v; want ErrNotFound", k, got, err)
		}
	}
	verifyScan := func(it *Iterator, err error, state map[string]string, lo, hi string) {
		t.Helper()
		got := scanAll(t, it, err)
		var want []string
		for _, k := range slices.Sorted(maps.Keys(state)) {
			if k >= lo && (hi == "" || k < hi) {
				want = append(want, k+"="+state[k])
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("scan [%s,%s): got %d entries, want %d", lo, hi, len(got), len(want))
		}
	}

	for i := 0; i < ops; i++ {
		switch r := rng.IntN(100); {
		case r < 45:
			k, v := key(), value()
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			model[k] = v
		case r < 60:
			k := key()
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
			delete(model, k)
		case r < 65:
			var b Batch
			for j := 0; j < 1+rng.IntN(20); j++ {
				k := key()
				if rng.IntN(3) == 0 {
					b.Delete([]byte(k))
					delete(model, k)
				} else {
					v := value()
					b.Put([]byte(k), []byte(v))
					model[k] = v
				}
			}
			if err := db.Apply(&b); err != nil {
				t.Fatal(err)
			}
		case r < 85:
			verifyGet(db.Get, model, key())
		case r < 88:
			lo := key()
			hi := ""
			if rng.IntN(2) == 0 {
				hi = key()
			}
			it, err := db.Scan(Range([]byte(lo), []byte(hi)))
			if hi == "" {
				it.Close()
				it, err = db.Scan(Range([]byte(lo), nil))
			}
			verifyScan(it, err, model, lo, hi)
		case r < 90:
			if len(snaps) < 3 {
				s, err := db.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				snaps = append(snaps, modelSnapshot{s, maps.Clone(model)})
			} else {
				j := rng.IntN(len(snaps))
				snaps[j].snap.Release()
				snaps = slices.Delete(snaps, j, j+1)
			}
		case r < 96:
			if len(snaps) > 0 {
				s := snaps[rng.IntN(len(snaps))]
				verifyGet(s.snap.Get, s.state, key())
				if rng.IntN(4) == 0 {
					it, err := s.snap.Scan(All())
					verifyScan(it, err, s.state, "", "")
				}
			}
		case r < 98:
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		default:
			for _, s := range snaps {
				s.snap.Release()
			}
			snaps = nil
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openDB(t, dir, opts...)
		}
	}

	for _, s := range snaps {
		s.snap.Release()
	}
	waitForCompactions(t, db)
	for k := 0; k < 800; k++ {
		verifyGet(db.Get, model, fmt.Sprintf("key-%04d", k))
	}
	it, err := db.Scan(All())
	verifyScan(it, err, model, "", "")

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, dir, opts...)
	it, err = db.Scan(All())
	verifyScan(it, err, model, "", "")
}
