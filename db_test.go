package lsmkv

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// smallOpts forces flushes and compactions with little data.
func smallOpts(extra ...Option) []Option {
	return append([]Option{
		WithMemtableSize(16 << 10),
		WithSyncPolicy(SyncNone),
		WithBlockSize(512),
		WithL0CompactionTrigger(2),
		WithLevelSizes(64<<10, 4),
		WithTargetFileSize(32 << 10),
	}, extra...)
}

func openDB(t testing.TB, dir string, opts ...Option) *DB {
	t.Helper()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func mustPut(t testing.TB, db *DB, k, v string) {
	t.Helper()
	if err := db.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func scanAll(t testing.TB, it *Iterator, err error) []string {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var out []string
	for it.Next() {
		out = append(out, string(it.Key())+"="+string(it.Value()))
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBasicOperations(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer db.Close()

	type step struct {
		op      string // put, del, get
		key     string
		val     string
		wantErr error
	}
	steps := []step{
		{"get", "a", "", ErrNotFound},
		{"put", "a", "1", nil},
		{"get", "a", "1", nil},
		{"put", "a", "2", nil},
		{"get", "a", "2", nil},
		{"del", "a", "", nil},
		{"get", "a", "", ErrNotFound},
		{"del", "never-written", "", nil},
		{"put", "", "empty key", nil},
		{"get", "", "empty key", nil},
		{"put", "nil-value", "", nil},
		{"get", "nil-value", "", nil},
	}
	for i, s := range steps {
		switch s.op {
		case "put":
			mustPut(t, db, s.key, s.val)
		case "del":
			if err := db.Delete([]byte(s.key)); err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
		case "get":
			got, err := db.Get([]byte(s.key))
			if !errors.Is(err, s.wantErr) {
				t.Fatalf("step %d: Get(%q) err = %v, want %v", i, s.key, err, s.wantErr)
			}
			if err == nil && string(got) != s.val {
				t.Fatalf("step %d: Get(%q) = %q, want %q", i, s.key, got, s.val)
			}
		}
	}
}

func TestGetReturnsCopy(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer db.Close()
	mustPut(t, db, "k", "value")
	v, _ := db.Get([]byte("k"))
	v[0] = 'X'
	if v2, _ := db.Get([]byte("k")); string(v2) != "value" {
		t.Fatalf("stored value mutated through Get result: %q", v2)
	}
}

func TestScanRanges(t *testing.T) {
	db := openDB(t, t.TempDir(), smallOpts()...)
	defer db.Close()
	for _, k := range []string{"app", "apple", "apricot", "banana", "blueberry", "cherry"} {
		mustPut(t, db, k, "v")
	}
	if err := db.Delete([]byte("apricot")); err != nil {
		t.Fatal(err)
	}
	// Half the data in tables, half in the memtable.
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "avocado", "v")
	mustPut(t, db, "banana", "v2")

	tests := []struct {
		name string
		r    KeyRange
		want string
	}{
		{"all", All(), "[app=v apple=v avocado=v banana=v2 blueberry=v cherry=v]"},
		{"prefix ap", Prefix([]byte("ap")), "[app=v apple=v]"},
		{"prefix b", Prefix([]byte("b")), "[banana=v2 blueberry=v]"},
		{"prefix none", Prefix([]byte("zz")), "[]"},
		{"range", Range([]byte("apple"), []byte("blueberry")), "[apple=v avocado=v banana=v2]"},
		{"open start", Range(nil, []byte("apple")), "[app=v]"},
		{"open end", Range([]byte("c"), nil), "[cherry=v]"},
		{"empty range", Range([]byte("b"), []byte("b")), "[]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it, err := db.Scan(tt.r)
			if got := fmt.Sprint(scanAll(t, it, err)); got != tt.want {
				t.Fatalf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestPrefixEnd(t *testing.T) {
	tests := []struct{ in, want []byte }{
		{[]byte("abc"), []byte("abd")},
		{[]byte{'a', 0xff}, []byte("b")},
		{[]byte{0xff, 0xff}, nil},
		{nil, nil},
	}
	for _, tt := range tests {
		if got := prefixEnd(tt.in); !bytes.Equal(got, tt.want) {
			t.Errorf("prefixEnd(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestIteratorIsStable(t *testing.T) {
	db := openDB(t, t.TempDir(), smallOpts()...)
	defer db.Close()
	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("k%03d", i), "old")
	}
	it, err := db.Scan(All())
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	// Writes, flushes and compactions after creation must not leak in.
	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("k%03d", i), "new")
		mustPut(t, db, fmt.Sprintf("k%03da", i), "added")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	n := 0
	for it.Next() {
		if string(it.Value()) != "old" {
			t.Fatalf("iterator saw %s=%s", it.Key(), it.Value())
		}
		n++
	}
	if n != 100 || it.Err() != nil {
		t.Fatalf("n=%d err=%v", n, it.Err())
	}
}

func TestSnapshot(t *testing.T) {
	db := openDB(t, t.TempDir(), smallOpts()...)
	defer db.Close()
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "1")
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "a", "2")
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "c", "2")

	// Push everything through flush and several compactions; the snapshot
	// must still see its versions.
	for i := 0; i < 3000; i++ {
		mustPut(t, db, fmt.Sprintf("filler-%05d", i), "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	waitForCompactions(t, db)

	check := func(get func([]byte) ([]byte, error), key, want string) {
		t.Helper()
		v, err := get([]byte(key))
		if want == "" {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get(%q) = %q, %v; want not found", key, v, err)
			}
			return
		}
		if err != nil || string(v) != want {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, v, err, want)
		}
	}
	check(snap.Get, "a", "1")
	check(snap.Get, "b", "1")
	check(snap.Get, "c", "")
	check(db.Get, "a", "2")
	check(db.Get, "b", "")
	check(db.Get, "c", "2")

	it, err := snap.Scan(Range(nil, []byte("c")))
	if got := fmt.Sprint(scanAll(t, it, err)); got != "[a=1 b=1]" {
		t.Fatalf("snapshot scan = %s", got)
	}
	snap.Release()
	snap.Release()
	if _, err := snap.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get on released snapshot: %v", err)
	}
}

func TestBatchAtomicVisibility(t *testing.T) {
	db := openDB(t, t.TempDir(), WithSyncPolicy(SyncNone))
	defer db.Close()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var b Batch
			v := []byte(fmt.Sprint(i))
			b.Put([]byte("x"), v)
			b.Put([]byte("y"), v)
			if err := db.Apply(&b); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 2000; i++ {
		snap, err := db.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		x, errX := snap.Get([]byte("x"))
		y, errY := snap.Get([]byte("y"))
		snap.Release()
		if !errors.Is(errX, errY) || !bytes.Equal(x, y) {
			t.Fatalf("torn batch: x=%q(%v) y=%q(%v)", x, errX, y, errY)
		}
	}
	close(stop)
	wg.Wait()
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	db := openDB(t, t.TempDir(), smallOpts()...)
	defer db.Close()
	const writers, perWriter = 4, 1500
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("w%d-%05d", w, i)
				if err := db.Put([]byte(k), []byte(k)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	done := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				k := fmt.Sprintf("w%d-%05d", r%writers, i%perWriter)
				v, err := db.Get([]byte(k))
				if err == nil && string(v) != k {
					t.Errorf("Get(%s) = %s", k, v)
					return
				}
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Error(err)
					return
				}
			}
		}(r)
	}
	time.Sleep(10 * time.Millisecond)
	// Wait for writers by polling the last keys, then stop readers.
	for w := 0; w < writers; w++ {
		for {
			if _, err := db.Get([]byte(fmt.Sprintf("w%d-%05d", w, perWriter-1))); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	close(done)
	wg.Wait()
	it, err := db.Scan(All())
	if n := len(scanAll(t, it, err)); n != writers*perWriter {
		t.Fatalf("scan found %d keys, want %d", n, writers*perWriter)
	}
}

func TestClosedDB(t *testing.T) {
	db := openDB(t, t.TempDir())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	checks := map[string]error{
		"Close":    db.Close(),
		"Put":      db.Put([]byte("k"), nil),
		"Delete":   db.Delete([]byte("k")),
		"Flush":    db.Flush(),
		"Get":      func() error { _, err := db.Get([]byte("k")); return err }(),
		"Scan":     func() error { _, err := db.Scan(All()); return err }(),
		"Snapshot": func() error { _, err := db.Snapshot(); return err }(),
		"Stats":    func() error { _, err := db.Stats(); return err }(),
	}
	for name, err := range checks {
		if !errors.Is(err, ErrClosed) {
			t.Errorf("%s after Close: err = %v, want ErrClosed", name, err)
		}
	}
}

func TestCompactionReducesTables(t *testing.T) {
	db := openDB(t, t.TempDir(), smallOpts()...)
	defer db.Close()
	val := bytes.Repeat([]byte("v"), 100)
	for round := 0; round < 5; round++ {
		for i := 0; i < 1000; i++ {
			if err := db.Put([]byte(fmt.Sprintf("key-%05d", i)), val); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 1000; i += 2 {
		if err := db.Delete([]byte(fmt.Sprintf("key-%05d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	waitForCompactions(t, db)
	st, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Compactions == 0 {
		t.Fatalf("no compactions ran: %+v", st)
	}
	if st.Levels[0].Tables >= 2 {
		t.Fatalf("level 0 still has %d tables", st.Levels[0].Tables)
	}
	var total uint64
	for _, l := range st.Levels {
		total += l.Bytes
	}
	// 500 live keys of ~115 bytes; five overwrites must have been merged away.
	if total > 200<<10 {
		t.Fatalf("tables hold %d bytes; shadowed versions were not dropped", total)
	}
	it, err := db.Scan(All())
	if n := len(scanAll(t, it, err)); n != 500 {
		t.Fatalf("scan found %d keys, want 500", n)
	}
}

// waitForCompactions polls until the picker has nothing left to do.
func waitForCompactions(t testing.TB, db *DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		db.mu.Lock()
		v := db.current.v
		imm := len(db.imm)
		db.mu.Unlock()
		busy := imm > 0
		for l := 0; l < len(v.Levels)-1 && !busy; l++ {
			busy = db.picker.Score(v, l) >= 1
		}
		if !busy {
			return
		}
		db.maybeScheduleCompaction()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("compactions did not settle")
}
