package lsmkv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nikrybak/lsmkv/internal/manifest"
)

// crashCopy copies the database directory while the DB is still open, which
// captures what a crash at this instant would leave on disk (log records are
// handed to the OS on every commit). Installs are paused so the copied
// manifest and tables agree; files that vanish meanwhile are already
// unreferenced, exactly as after a real crash.
func crashCopy(t *testing.T, db *DB) string {
	t.Helper()
	waitForCompactions(t, db)
	db.installMu.Lock()
	defer db.installMu.Unlock()
	src := db.dir
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func newestLog(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var logs []uint64
	for _, e := range entries {
		if kind, num := parseFileName(e.Name()); kind == fileLog {
			logs = append(logs, num)
		}
	}
	if len(logs) == 0 {
		t.Fatal("no log file")
	}
	return logPath(dir, slices.Max(logs))
}

func TestRecoverAfterCrash(t *testing.T) {
	src := t.TempDir()
	db := openDB(t, src, smallOpts()...)
	for i := 0; i < 2000; i++ {
		mustPut(t, db, fmt.Sprintf("k%05d", i), fmt.Sprint(i))
	}
	for i := 0; i < 2000; i += 3 {
		if err := db.Delete([]byte(fmt.Sprintf("k%05d", i))); err != nil {
			t.Fatal(err)
		}
	}
	dir := crashCopy(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	re := openDB(t, dir, smallOpts()...)
	defer re.Close()
	for i := 0; i < 2000; i++ {
		v, err := re.Get([]byte(fmt.Sprintf("k%05d", i)))
		if i%3 == 0 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted key k%05d resurrected: %q %v", i, v, err)
			}
			continue
		}
		if err != nil || string(v) != fmt.Sprint(i) {
			t.Fatalf("k%05d = %q, %v", i, v, err)
		}
	}
	// New writes after recovery must win over recovered ones.
	mustPut(t, re, "k00001", "after")
	if v, _ := re.Get([]byte("k00001")); string(v) != "after" {
		t.Fatalf("post-recovery write lost: %q", v)
	}
}

func TestRecoverTornLogTail(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(t *testing.T, path string)
		wantLost int // trailing writes expected to be lost
	}{
		{"intact", func(*testing.T, string) {}, 0},
		{"truncated mid record", func(t *testing.T, path string) { truncateBy(t, path, 3) }, 1},
		{"truncated mid header", func(t *testing.T, path string) {
			// Each record here is 8 (header) + 12 (batch header) + op bytes.
			truncateBy(t, path, recordSize("k099", "v099")+4)
		}, 2},
		{"garbage tail", func(t *testing.T, path string) {
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x10, 0, 0, 0, 1, 2}); err != nil {
				t.Fatal(err)
			}
		}, 0},
		{"flipped bit in last record", func(t *testing.T, path string) { flipByte(t, path, -1) }, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := t.TempDir()
			db := openDB(t, src, WithSyncPolicy(SyncAlways))
			for i := 0; i < 100; i++ {
				mustPut(t, db, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
			}
			dir := crashCopy(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			tt.mutate(t, newestLog(t, dir))

			re := openDB(t, dir)
			defer re.Close()
			for i := 0; i < 100; i++ {
				_, err := re.Get([]byte(fmt.Sprintf("k%03d", i)))
				lost := i >= 100-tt.wantLost
				if lost != errors.Is(err, ErrNotFound) {
					t.Fatalf("k%03d: err=%v, expected lost=%v", i, err, lost)
				}
			}
		})
	}
}

func recordSize(k, v string) int64 {
	// wal header + batch header + kind + uvarint lengths + bytes
	return int64(8 + 12 + 1 + 1 + len(k) + 1 + len(v))
}

func truncateBy(t *testing.T, path string, n int64) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, st.Size()-n); err != nil {
		t.Fatal(err)
	}
}

func flipByte(t *testing.T, path string, off int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if off < 0 {
		off += len(data)
	}
	data[off] ^= 0x01
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestMidLogCorruptionFailsOpen(t *testing.T) {
	src := t.TempDir()
	db := openDB(t, src, WithSyncPolicy(SyncAlways))
	for i := 0; i < 50; i++ {
		mustPut(t, db, fmt.Sprintf("k%03d", i), "value")
	}
	dir := crashCopy(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	flipByte(t, newestLog(t, dir), 30)
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt", err)
	}
}

func TestCorruptTableFailsOpen(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, smallOpts()...)
	for i := 0; i < 500; i++ {
		mustPut(t, db, fmt.Sprintf("k%03d", i), "value")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, _, err := manifest.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	var num uint64
	for _, files := range st.Version.Levels {
		for _, f := range files {
			num = f.Num
		}
	}
	flipByte(t, tablePath(dir, num), -3) // inside the magic number
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt", err)
	}
}

func TestCorruptManifestFailsOpen(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	mustPut(t, db, "k", "v")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	flipByte(t, filepath.Join(dir, manifest.FileName), 12)
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt", err)
	}
}

func TestOpenRemovesOrphans(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	mustPut(t, db, "k", "v")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Leftovers of a crash in the middle of a compaction or a manifest write.
	orphan := tablePath(dir, 999)
	if err := os.WriteFile(orphan, []byte("partial table"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifest.FileName+".tmp"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	re := openDB(t, dir)
	defer re.Close()
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan table not removed: %v", err)
	}
	if v, err := re.Get([]byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("Get = %q, %v", v, err)
	}
	// File numbers must not collide with the orphan's.
	if n := re.nextFileNum.Load(); n <= 999 {
		t.Fatalf("next file number %d reuses orphan numbers", n)
	}
}

func TestReopenManyTimes(t *testing.T) {
	dir := t.TempDir()
	for round := 0; round < 8; round++ {
		db := openDB(t, dir, smallOpts()...)
		for i := 0; i < 300; i++ {
			mustPut(t, db, fmt.Sprintf("k%03d", i), fmt.Sprintf("r%d", round))
		}
		if round%2 == 0 {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	db := openDB(t, dir, smallOpts()...)
	defer db.Close()
	for i := 0; i < 300; i++ {
		if v, err := db.Get([]byte(fmt.Sprintf("k%03d", i))); err != nil || string(v) != "r7" {
			t.Fatalf("k%03d = %q, %v", i, v, err)
		}
	}
}
