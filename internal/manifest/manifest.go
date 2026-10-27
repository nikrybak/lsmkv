// Package manifest records which SSTables are live and at which level, and
// persists that state atomically.
//
// The manifest is a single small file rewritten in full on every change:
// write MANIFEST.tmp, fsync it, rename it over MANIFEST, fsync the directory.
// Rename is atomic on POSIX filesystems and on NTFS, so after a crash the
// directory holds either the old or the new manifest, never a mix. Rewriting
// in full is O(number of tables), which stays small for an embedded engine and
// avoids the log-compaction machinery of an append-only manifest.
//
// Encoding: magic "LSMKVMF1" | payload length u32 | payload | crc32c u32, where
// payload is a sequence of uvarints and length-prefixed byte strings.
package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"

	"github.com/nikrybak/lsmkv/internal/base"
)

// NumLevels is the depth of the LSM tree.
const NumLevels = 7

// FileName is the name of the manifest inside the database directory.
const FileName = "MANIFEST"

const (
	magic   = "LSMKVMF1"
	tmpName = FileName + ".tmp"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// FileMeta describes one SSTable.
type FileMeta struct {
	Num      uint64
	Size     uint64
	Smallest []byte // internal key
	Largest  []byte // internal key
}

// SmallestUser returns the smallest user key in the table.
func (f *FileMeta) SmallestUser() []byte { return base.UserKey(f.Smallest) }

// LargestUser returns the largest user key in the table.
func (f *FileMeta) LargestUser() []byte { return base.UserKey(f.Largest) }

// Overlaps reports whether the table's user-key range intersects [lo, hi].
// A nil bound is unbounded.
func (f *FileMeta) Overlaps(lo, hi []byte) bool {
	if hi != nil && bytes.Compare(f.SmallestUser(), hi) > 0 {
		return false
	}
	if lo != nil && bytes.Compare(f.LargestUser(), lo) < 0 {
		return false
	}
	return true
}

// Version is an immutable snapshot of the table set. Level 0 tables may
// overlap and are ordered newest first; tables in deeper levels are disjoint
// and ordered by key.
type Version struct {
	Levels [NumLevels][]*FileMeta
}

// LevelFile names a table together with its level.
type LevelFile struct {
	Level int
	Meta  *FileMeta
}

// Edit is a change to a Version.
type Edit struct {
	Added   []LevelFile
	Deleted map[uint64]bool // table numbers
}

// Apply returns a new Version with e applied. v is not modified.
func (v *Version) Apply(e Edit) (*Version, error) {
	nv := &Version{}
	for l, files := range v.Levels {
		for _, f := range files {
			if !e.Deleted[f.Num] {
				nv.Levels[l] = append(nv.Levels[l], f)
			}
		}
	}
	for _, a := range e.Added {
		if a.Level < 0 || a.Level >= NumLevels {
			return nil, fmt.Errorf("manifest: level %d out of range", a.Level)
		}
		nv.Levels[a.Level] = append(nv.Levels[a.Level], a.Meta)
	}
	nv.sort()
	if err := nv.check(); err != nil {
		return nil, err
	}
	return nv, nil
}

func (v *Version) sort() {
	slices.SortFunc(v.Levels[0], func(a, b *FileMeta) int { return cmpUint(b.Num, a.Num) })
	for l := 1; l < NumLevels; l++ {
		slices.SortFunc(v.Levels[l], func(a, b *FileMeta) int { return base.Compare(a.Smallest, b.Smallest) })
	}
}

// check enforces the invariant the read path relies on: tables in levels
// >= 1 are disjoint in user-key space.
func (v *Version) check() error {
	for l := 1; l < NumLevels; l++ {
		files := v.Levels[l]
		for i := 1; i < len(files); i++ {
			if bytes.Compare(files[i-1].LargestUser(), files[i].SmallestUser()) >= 0 {
				return fmt.Errorf("manifest: level %d tables %d and %d overlap: %w",
					l, files[i-1].Num, files[i].Num, base.ErrCorrupt)
			}
		}
	}
	return nil
}

// Overlapping returns the tables of level whose user-key range intersects
// [lo, hi].
func (v *Version) Overlapping(level int, lo, hi []byte) []*FileMeta {
	var out []*FileMeta
	for _, f := range v.Levels[level] {
		if f.Overlaps(lo, hi) {
			out = append(out, f)
		}
	}
	return out
}

// LevelSize returns the total size in bytes of the tables in level.
func (v *Version) LevelSize(level int) uint64 {
	var n uint64
	for _, f := range v.Levels[level] {
		n += f.Size
	}
	return n
}

// Files returns every table number in the version.
func (v *Version) Files() map[uint64]bool {
	out := make(map[uint64]bool)
	for _, files := range v.Levels {
		for _, f := range files {
			out[f.Num] = true
		}
	}
	return out
}

// State is everything the manifest persists.
type State struct {
	NextFileNum uint64 // next unused file number for tables and logs
	LogNum      uint64 // logs numbered below this are fully flushed
	LastSeq     uint64 // highest sequence number stored in any table
	Version     *Version
}

// Store atomically replaces the manifest in dir with s.
func Store(dir string, s State) error {
	data := encode(s)
	tmp := filepath.Join(dir, tmpName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("manifest: create temp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("manifest: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("manifest: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest: close: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, FileName)); err != nil {
		return fmt.Errorf("manifest: rename: %w", err)
	}
	return SyncDir(dir)
}

// Load reads the manifest in dir. ok is false when none exists yet.
func Load(dir string) (s State, ok bool, err error) {
	data, err := os.ReadFile(filepath.Join(dir, FileName))
	if errors.Is(err, fs.ErrNotExist) {
		return State{Version: &Version{}}, false, nil
	}
	if err != nil {
		return State{}, false, fmt.Errorf("manifest: read: %w", err)
	}
	s, err = decode(data)
	if err != nil {
		return State{}, false, err
	}
	return s, true, nil
}

// SyncDir makes directory entries (creates, renames, removes) durable.
// Windows cannot fsync a directory handle; NTFS journals metadata itself, so
// the call is skipped there.
func SyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("manifest: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("manifest: fsync dir: %w", err)
	}
	return nil
}

func encode(s State) []byte {
	var p []byte
	p = binary.AppendUvarint(p, s.NextFileNum)
	p = binary.AppendUvarint(p, s.LogNum)
	p = binary.AppendUvarint(p, s.LastSeq)
	var n uint64
	for _, files := range s.Version.Levels {
		n += uint64(len(files))
	}
	p = binary.AppendUvarint(p, n)
	for l, files := range s.Version.Levels {
		for _, f := range files {
			p = binary.AppendUvarint(p, uint64(l))
			p = binary.AppendUvarint(p, f.Num)
			p = binary.AppendUvarint(p, f.Size)
			p = appendBytes(p, f.Smallest)
			p = appendBytes(p, f.Largest)
		}
	}
	out := append([]byte(magic), binary.LittleEndian.AppendUint32(nil, uint32(len(p)))...)
	out = append(out, p...)
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(p, castagnoli))
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func corrupt(what string) error { return fmt.Errorf("manifest: %s: %w", what, base.ErrCorrupt) }

func decode(data []byte) (State, error) {
	if len(data) < len(magic)+8 || string(data[:len(magic)]) != magic {
		return State{}, corrupt("bad header")
	}
	n := binary.LittleEndian.Uint32(data[len(magic):])
	body := data[len(magic)+4:]
	if uint64(n)+4 != uint64(len(body)) {
		return State{}, corrupt("length mismatch")
	}
	p := body[:n]
	if binary.LittleEndian.Uint32(body[n:]) != crc32.Checksum(p, castagnoli) {
		return State{}, corrupt("checksum mismatch")
	}
	d := decoder{buf: p}
	s := State{NextFileNum: d.uvarint(), LogNum: d.uvarint(), LastSeq: d.uvarint(), Version: &Version{}}
	count := d.uvarint()
	for i := uint64(0); i < count && d.err == nil; i++ {
		level := d.uvarint()
		f := &FileMeta{Num: d.uvarint(), Size: d.uvarint(), Smallest: d.bytes(), Largest: d.bytes()}
		if d.err == nil && (level >= NumLevels || !base.Valid(f.Smallest) || !base.Valid(f.Largest)) {
			d.err = corrupt("bad table entry")
		}
		if d.err == nil {
			s.Version.Levels[level] = append(s.Version.Levels[level], f)
		}
	}
	if d.err == nil && len(d.buf) != 0 {
		d.err = corrupt("trailing bytes")
	}
	if d.err != nil {
		return State{}, d.err
	}
	s.Version.sort()
	if err := s.Version.check(); err != nil {
		return State{}, err
	}
	return s, nil
}

type decoder struct {
	buf []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.err = corrupt("bad varint")
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.buf)) {
		d.err = corrupt("string overflows payload")
		return nil
	}
	b := bytes.Clone(d.buf[:n])
	d.buf = d.buf[n:]
	return b
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
