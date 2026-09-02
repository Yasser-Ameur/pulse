// Package snapshot implements durable recovery checkpoints for a partition
// log (docs/Storage.md §8).
//
// A snapshot is a tiny, atomically-written record of the log's LEO and active
// segment state. Recovery consults it to skip the scan-from-epoch: sealed
// segments are restored from their persisted index files and the active
// segment's tail is trusted exactly when it matches the recorded size. A
// missing, torn, or stale snapshot always falls back to the full scan-based
// recovery, so correctness never depends on a snapshot being present.
package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/checksum"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/filesystem"
)

// Snapshot format constants. The file is 40 bytes, all big-endian.
const (
	// Magic identifies the snapshot format family.
	Magic byte = 0x53 // 'S'
	// Version is the snapshot format version.
	Version byte = 0x01
	// FileSize is the byte length of a version 0x01 snapshot.
	FileSize = 40
)

// ErrCorrupt reports a snapshot that cannot be trusted.
var ErrCorrupt = errors.New("corrupt snapshot")

// State is one durable checkpoint of a partition log.
type State struct {
	// LEO is the log's next offset. Offsets are contiguous under the single
	// writer lock, so LEO is also the high watermark.
	LEO offset.Offset
	// ActiveBase is the base offset of the active (last) segment.
	ActiveBase offset.Offset
	// ActiveSize is the active segment's valid byte length at checkpoint time.
	ActiveSize int64
	// ActiveNext is the active segment's next offset; always equals LEO.
	ActiveNext offset.Offset
}

// Path returns the snapshot file path for a partition directory.
func Path(dir string) string { return filepath.Join(dir, "snapshot") }

// Write durably persists st for the partition at dir. The write is atomic:
// the data is written to a temp file, fsynced, and renamed into place, so a
// crash leaves either the previous snapshot or the new one, never a torn mix.
func Write(dir string, st State) error {
	if !st.LEO.Valid() || !st.ActiveBase.Valid() || st.ActiveNext != st.LEO ||
		st.ActiveBase > st.LEO || st.ActiveSize < 0 {
		return fmt.Errorf("%w: invalid checkpoint state", os.ErrInvalid)
	}
	buf := make([]byte, FileSize)
	buf[0] = Magic
	buf[1] = Version
	binary.BigEndian.PutUint64(buf[8:16], uint64(st.LEO))
	binary.BigEndian.PutUint64(buf[16:24], uint64(st.ActiveBase))
	binary.BigEndian.PutUint64(buf[24:32], uint64(st.ActiveSize))
	binary.BigEndian.PutUint64(buf[32:40], uint64(st.ActiveNext))
	binary.BigEndian.PutUint32(buf[4:8], checksum.Sum(buf[8:]))
	return filesystem.AtomicWriteFile(Path(dir), buf)
}

// Load reads the checkpoint for the partition at dir. present is false when
// no snapshot file exists; a present-but-untrustworthy snapshot returns
// ErrCorrupt so the caller can discard it and fall back to a full scan.
func Load(dir string) (State, bool, error) {
	data, err := os.ReadFile(Path(dir))
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	if len(data) != FileSize {
		return State{}, true, fmt.Errorf("%w: %d bytes", ErrCorrupt, len(data))
	}
	if data[0] != Magic || data[1] != Version {
		return State{}, true, fmt.Errorf("%w: bad magic/version", ErrCorrupt)
	}
	if binary.BigEndian.Uint32(data[4:8]) != checksum.Sum(data[8:]) {
		return State{}, true, fmt.Errorf("%w: crc mismatch", ErrCorrupt)
	}
	st := State{
		LEO:        offset.Offset(binary.BigEndian.Uint64(data[8:16])),
		ActiveBase: offset.Offset(binary.BigEndian.Uint64(data[16:24])),
		ActiveSize: int64(binary.BigEndian.Uint64(data[24:32])),
		ActiveNext: offset.Offset(binary.BigEndian.Uint64(data[32:40])),
	}
	if !st.LEO.Valid() || !st.ActiveBase.Valid() || st.ActiveNext != st.LEO ||
		st.ActiveBase > st.LEO || st.ActiveSize < 0 {
		return State{}, true, fmt.Errorf("%w: inconsistent state", ErrCorrupt)
	}
	return st, true, nil
}
