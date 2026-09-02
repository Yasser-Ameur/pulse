// Package filesystem owns the data-plane file layout: path construction, safe
// file opens, segment naming, and the few durability helpers the engine needs.
// All other storage packages treat files as abstract byte streams, per
// docs/Storage.md §10.
package filesystem

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

// ErrNotFound is returned when a partition has no data on disk. The engine
// wraps it with ports.ErrLogNotFound so recovery can recreate the log.
var ErrNotFound = errors.New("partition log not found")

// SegmentNameWidth is the zero-padded width of segment base offsets in file
// names. 20 digits keeps lexicographic order identical to offset order.
const SegmentNameWidth = 20

// DataDir returns the topic data directory.
func DataDir(root string) string { return filepath.Join(root, "topics") }

// MetaDir returns the metadata store directory.
func MetaDir(root string) string { return filepath.Join(root, "meta") }

// TopicDir returns the directory holding a topic's partitions.
func TopicDir(root string, name topic.Name) string {
	return filepath.Join(DataDir(root), name.String())
}

// PartitionDir returns the directory holding a partition's segments.
func PartitionDir(root string, name topic.Name, id partition.ID) string {
	return filepath.Join(TopicDir(root, name), strconv.FormatInt(int64(id), 10))
}

// SegmentLogPath returns the data file path for a segment.
func SegmentLogPath(dir string, base offset.Offset) string {
	return filepath.Join(dir, SegmentName(base)+".log")
}

// SegmentIndexPath returns the index file path for a segment.
func SegmentIndexPath(dir string, base offset.Offset) string {
	return filepath.Join(dir, SegmentName(base)+".index")
}

// SegmentName formats a segment base offset as a zero-padded file base name.
func SegmentName(base offset.Offset) string {
	return fmt.Sprintf("%0*d", SegmentNameWidth, int64(base))
}

// ParseSegmentName extracts the base offset from a segment file base name.
func ParseSegmentName(base string) (offset.Offset, error) {
	if len(base) != SegmentNameWidth {
		return 0, fmt.Errorf("%w: invalid segment name %q", ErrNotFound, base)
	}
	for _, r := range base {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%w: invalid segment name %q", ErrNotFound, base)
		}
	}
	n, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid segment name %q", ErrNotFound, base)
	}
	return offset.Offset(n), nil
}

// OpenDataFile opens (creating if needed) a segment data file. Data files are
// opened without O_TRUNC so a reopened log never loses its tail.
func OpenDataFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
}

// MkdirAll creates the partition directory if it does not exist.
func MkdirAll(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

// PartitionExists reports whether a partition has data on disk.
func PartitionExists(root string, name topic.Name, id partition.ID) bool {
	_, err := os.Stat(PartitionDir(root, name, id))
	return err == nil
}

// SyncFile fsyncs a file to stable storage.
func SyncFile(f *os.File) error {
	return f.Sync()
}

// SyncDir fsyncs a directory so that renames and creates inside it are
// durable. Best-effort on platforms that disallow it.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		if runtime.GOOS == "windows" {
			// Windows cannot fsync a directory handle; the data files are
			// already synced, so renames are best-effort durable.
			return nil
		}
		return err
	}
	return nil
}

// AtomicWriteFile writes data to a temp file in the same directory and
// renames it over path, then fsyncs both the file and the directory. Used for
// index rebuilds so a crash never leaves a half-written index.
func AtomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-index-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return SyncDir(dir)
}

// SegmentFiles returns the sorted (by name) .log file paths in dir.
func SegmentFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out)
	return out, nil
}
