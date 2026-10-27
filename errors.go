package lsmkv

import (
	"errors"

	"github.com/nikrybak/lsmkv/internal/base"
)

var (
	// ErrNotFound is returned by Get when the key has no live value.
	ErrNotFound = errors.New("lsmkv: not found")
	// ErrClosed is returned by every operation on a closed DB, snapshot or
	// iterator.
	ErrClosed = errors.New("lsmkv: closed")
	// ErrCorrupt wraps every checksum, format or invariant violation found
	// in persisted data. Test for it with errors.Is.
	ErrCorrupt = base.ErrCorrupt
)
