package lsmkv

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

type fileKind int

const (
	fileUnknown fileKind = iota
	fileLog
	fileTable
)

func logPath(dir string, num uint64) string { return filepath.Join(dir, fmt.Sprintf("%06d.log", num)) }
func tablePath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.sst", num))
}

func parseFileName(name string) (fileKind, uint64) {
	stem, ext, ok := strings.Cut(name, ".")
	if !ok {
		return fileUnknown, 0
	}
	num, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return fileUnknown, 0
	}
	switch ext {
	case "log":
		return fileLog, num
	case "sst":
		return fileTable, num
	}
	return fileUnknown, 0
}
