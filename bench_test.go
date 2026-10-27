package lsmkv

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
)

const benchKeys = 100_000

var benchValue = make([]byte, 100)

func benchKey(i int) []byte { return []byte(fmt.Sprintf("key-%010d", i)) }

func BenchmarkPut(b *testing.B) {
	for _, p := range []struct {
		name   string
		policy SyncPolicy
	}{{"SyncNone", SyncNone}, {"SyncAlways", SyncAlways}} {
		b.Run(p.name, func(b *testing.B) {
			db := openDB(b, b.TempDir(), WithSyncPolicy(p.policy))
			defer db.Close()
			rng := rand.New(rand.NewPCG(1, 2))
			b.SetBytes(int64(len(benchValue) + 14))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Put(benchKey(rng.IntN(1<<30)), benchValue); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPutParallel shows group commit: under SyncAlways, concurrent
// writers share fsyncs, so throughput scales with the number of writers.
func BenchmarkPutParallel(b *testing.B) {
	db := openDB(b, b.TempDir(), WithSyncPolicy(SyncAlways))
	defer db.Close()
	b.SetParallelism(16)
	b.SetBytes(int64(len(benchValue) + 14))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewPCG(rand.Uint64(), 0))
		for pb.Next() {
			if err := db.Put(benchKey(rng.IntN(1<<30)), benchValue); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// loadedDB returns a DB holding benchKeys keys, all flushed and compacted.
func loadedDB(b *testing.B) *DB {
	b.Helper()
	db := openDB(b, b.TempDir(), WithSyncPolicy(SyncNone), WithMemtableSize(1<<20))
	var batch Batch
	for i := 0; i < benchKeys; i++ {
		batch.Put(benchKey(i), benchValue)
		if batch.Len() == 1000 {
			if err := db.Apply(&batch); err != nil {
				b.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	waitForCompactions(b, db)
	return db
}

func BenchmarkGet(b *testing.B) {
	db := loadedDB(b)
	defer db.Close()
	b.Run("Hit", func(b *testing.B) {
		rng := rand.New(rand.NewPCG(3, 4))
		for i := 0; i < b.N; i++ {
			if _, err := db.Get(benchKey(rng.IntN(benchKeys))); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Miss", func(b *testing.B) {
		rng := rand.New(rand.NewPCG(5, 6))
		for i := 0; i < b.N; i++ {
			k := benchKey(benchKeys + rng.IntN(benchKeys))
			if _, err := db.Get(k); !errors.Is(err, ErrNotFound) {
				b.Fatal(err)
			}
		}
	})
	b.Run("HitParallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewPCG(rand.Uint64(), 0))
			for pb.Next() {
				if _, err := db.Get(benchKey(rng.IntN(benchKeys))); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func BenchmarkGetMemtable(b *testing.B) {
	db := openDB(b, b.TempDir(), WithSyncPolicy(SyncNone), WithMemtableSize(64<<20))
	defer db.Close()
	for i := 0; i < benchKeys; i++ {
		if err := db.Put(benchKey(i), benchValue); err != nil {
			b.Fatal(err)
		}
	}
	rng := rand.New(rand.NewPCG(7, 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(benchKey(rng.IntN(benchKeys))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScan measures a 100-key range scan starting at a random key.
func BenchmarkScan(b *testing.B) {
	db := loadedDB(b)
	defer db.Close()
	rng := rand.New(rand.NewPCG(9, 10))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := rng.IntN(benchKeys - 100)
		it, err := db.Scan(Range(benchKey(start), nil))
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for n < 100 && it.Next() {
			n++
		}
		if n != 100 {
			b.Fatalf("scanned %d keys", n)
		}
		it.Close()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*100), "ns/key")
}
