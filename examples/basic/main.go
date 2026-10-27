// Command basic shows the lsmkv library API: writes, batches, point reads,
// prefix scans and snapshots.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/nikrybak/lsmkv"
)

func main() {
	dir, err := os.MkdirTemp("", "lsmkv-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := lsmkv.Open(filepath.Join(dir, "db"), lsmkv.WithSyncPolicy(lsmkv.SyncAlways))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Single writes.
	must(db.Put([]byte("user:1"), []byte("alice")))
	must(db.Put([]byte("user:2"), []byte("bob")))

	// An atomic batch: both operations survive a crash, or neither does.
	var b lsmkv.Batch
	b.Put([]byte("user:3"), []byte("carol"))
	b.Delete([]byte("user:2"))
	must(db.Apply(&b))

	// A snapshot freezes the current view.
	snap, err := db.Snapshot()
	must(err)
	defer snap.Release()
	must(db.Put([]byte("user:1"), []byte("alice v2")))

	v, err := db.Get([]byte("user:1"))
	must(err)
	old, err := snap.Get([]byte("user:1"))
	must(err)
	fmt.Printf("user:1 now=%q at snapshot=%q\n", v, old)

	if _, err := db.Get([]byte("user:2")); errors.Is(err, lsmkv.ErrNotFound) {
		fmt.Println("user:2 deleted")
	}

	it, err := db.Scan(lsmkv.Prefix([]byte("user:")))
	must(err)
	for it.Next() {
		fmt.Printf("%s = %s\n", it.Key(), it.Value())
	}
	must(it.Err())
	must(it.Close())

	st, err := db.Stats()
	must(err)
	fmt.Printf("last sequence %d, memtable %d bytes\n", st.LastSequence, st.MemtableBytes)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
