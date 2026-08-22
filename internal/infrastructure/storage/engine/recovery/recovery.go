// Package recovery rebuilds a partition's log state at startup.
//
// Every segment is scanned batch by batch: batch frames are decoded (magic,
// version, length, continuity, CRC) and the sparse index is rebuilt from the
// data. A torn or corrupted tail on the active (last) segment is truncated at
// the previous valid batch boundary; any corruption in a sealed segment is
// fatal, because only the log tail can legitimately be damaged by a crash.
// See docs/Storage.md §7.
//
// A durable snapshot (docs/Storage.md §8) shortcuts this scan: when a valid
// snapshot matches the on-disk active segment, sealed segments are restored
// from their persisted index files (the first batch's base offset is spot
// checked against the segment name) and the active segment's tail is trusted
// exactly at the recorded size. If the active segment grew past the snapshot,
// only the delta tail is scanned for torn writes. The snapshot is an
// optimization; a missing, torn, or stale snapshot falls back to the full scan
// below, which remains the correctness baseline.
package recovery

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/index"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/segment"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/snapshot"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/filesystem"
)

// Result carries the outcome of a recovery run.
type Result struct {
	// Segments are the recovered segments in offset order.
	Segments []*segment.Segment
	// Truncated reports that the active segment had a torn tail removed.
	Truncated bool
	// TruncatedBytes is the number of bytes removed from the active segment.
	TruncatedBytes int64
	// SnapshotUsed reports that recovery trusted a durable snapshot.
	SnapshotUsed bool
}

// Run recovers the partition log in dir, preferring the snapshot fast path
// when a valid snapshot matches the on-disk state. An empty directory yields
// no segments.
func Run(dir string, indexInterval int64) (*Result, error) {
	st, present, err := snapshot.Load(dir)
	if err != nil {
		if errors.Is(err, snapshot.ErrCorrupt) {
			// Untrustworthy checkpoint: discard it and fall back to a scan.
			_ = os.Remove(snapshot.Path(dir))
		}
		return runFull(dir, indexInterval)
	}
	if present {
		res, ok, err := runWithSnapshot(dir, indexInterval, st)
		if err != nil {
			return nil, err
		}
		if ok {
			return res, nil
		}
		// Snapshot did not match the on-disk state; scan everything.
		_ = os.Remove(snapshot.Path(dir))
	}
	return runFull(dir, indexInterval)
}

// runFull scans every segment and rebuilds state from the data. This is the
// correctness baseline used whenever no trustworthy snapshot exists.
func runFull(dir string, indexInterval int64) (*Result, error) {
	files, err := filesystem.SegmentFiles(dir)
	if err != nil {
		return nil, err
	}
	res := &Result{}
	for i, path := range files {
		last := i == len(files)-1
		base, err := filesystem.ParseSegmentName(filepath.Base(trimExt(path)))
		if err != nil {
			return nil, err
		}
		seg, err := segment.Open(path, base, indexInterval)
		if err != nil {
			return nil, err
		}
		size, err := seg.File().Stat()
		if err != nil {
			_ = seg.Close()
			return nil, err
		}
		nextOffset, entries, validSize, torn, err := scan(seg.File(), size.Size(), base, indexInterval, last)
		if err != nil {
			_ = seg.Close()
			return nil, fmt.Errorf("recover %s: %w", path, err)
		}
		if torn {
			res.Truncated = true
			res.TruncatedBytes += size.Size() - validSize
			if err := seg.TruncateTo(validSize, nextOffset); err != nil {
				_ = seg.Close()
				return nil, fmt.Errorf("recover %s: %w", path, err)
			}
		}
		if err := seg.RecoverFrom(validSize, nextOffset, entries); err != nil {
			_ = seg.Close()
			return nil, fmt.Errorf("recover %s: %w", path, err)
		}
		res.Segments = append(res.Segments, seg)
	}
	return res, nil
}

// runWithSnapshot recovers with st as the trusted checkpoint. It returns
// ok=false when the snapshot does not describe the current on-disk state, so
// the caller discards the snapshot and falls back to runFull.
func runWithSnapshot(dir string, indexInterval int64, st snapshot.State) (res *Result, ok bool, err error) {
	files, err := filesystem.SegmentFiles(dir)
	if err != nil {
		return nil, false, err
	}
	if len(files) == 0 {
		return nil, false, nil
	}
	bases := make([]offset.Offset, len(files))
	for i, path := range files {
		b, err := filesystem.ParseSegmentName(filepath.Base(trimExt(path)))
		if err != nil {
			return nil, false, nil
		}
		bases[i] = b
	}
	last := len(files) - 1
	if bases[last] != st.ActiveBase {
		return nil, false, nil // checkpoint describes a different active segment
	}

	res = &Result{SnapshotUsed: true}
	var opened []*segment.Segment
	defer func() {
		if err != nil || !ok {
			for _, s := range opened {
				_ = s.Close()
			}
		}
	}()

	for i, path := range files[:last] {
		seg, restored, err := restoreFromIndex(path, bases[i], bases[i+1], indexInterval)
		if err != nil {
			return nil, false, err
		}
		if !restored {
			sr, err := restoreByScan(path, bases[i], indexInterval, false)
			if err != nil {
				return nil, false, fmt.Errorf("recover %s: %w", path, err)
			}
			seg = sr.seg
		}
		opened = append(opened, seg)
		res.Segments = append(res.Segments, seg)
	}

	activePath := files[last]
	activeSize, err := fileSize(activePath)
	if err != nil {
		return nil, false, err
	}
	if activeSize < st.ActiveSize {
		return nil, false, nil // data is shorter than the checkpoint
	}

	var active *segment.Segment
	switch activeSize == st.ActiveSize {
	case true:
		// The tail matches the checkpoint exactly: trust it, restoring the
		// index from the persisted index file.
		seg, restored, err := restoreFromIndex(activePath, st.ActiveBase, st.LEO, indexInterval)
		if err != nil {
			return nil, false, err
		}
		if !restored {
			sr, err := restoreByScan(activePath, st.ActiveBase, indexInterval, true)
			if err != nil {
				return nil, false, fmt.Errorf("recover %s: %w", activePath, err)
			}
			seg = sr.seg
			res.Truncated = res.Truncated || sr.truncated
			res.TruncatedBytes += sr.truncBytes
		}
		active = seg
	default:
		// The active segment grew past the checkpoint. The prefix up to
		// st.ActiveSize was durable when the checkpoint was written, so it is
		// trusted; only the delta tail is scanned.
		prefix, loaded, err := loadPrefixEntries(activePath, st.ActiveBase, st.ActiveSize)
		if err != nil {
			return nil, false, err
		}
		if !loaded {
			sr, err := restoreByScan(activePath, st.ActiveBase, indexInterval, true)
			if err != nil {
				return nil, false, fmt.Errorf("recover %s: %w", activePath, err)
			}
			active = sr.seg
			res.Truncated = res.Truncated || sr.truncated
			res.TruncatedBytes += sr.truncBytes
			opened = append(opened, active)
			res.Segments = append(res.Segments, active)
			return res, true, nil
		}
		seg, err := segment.Open(activePath, st.ActiveBase, indexInterval)
		if err != nil {
			return nil, false, err
		}
		next, tail, validSize, torn, err := scanFrom(seg.File(), st.ActiveSize, activeSize, st.ActiveBase, st.LEO, indexInterval, true)
		if err != nil {
			_ = seg.Close()
			return nil, false, err
		}
		if torn {
			res.Truncated = true
			res.TruncatedBytes += activeSize - validSize
			if err := seg.TruncateTo(validSize, next); err != nil {
				_ = seg.Close()
				return nil, false, err
			}
		}
		if err := seg.RecoverFrom(validSize, next, append(prefix, tail...)); err != nil {
			_ = seg.Close()
			return nil, false, err
		}
		active = seg
	}

	opened = append(opened, active)
	res.Segments = append(res.Segments, active)
	return res, true, nil
}

// restoreFromIndex restores a segment from its persisted index file. restored
// is false when the index is missing or corrupt, so the caller falls back to
// restoreByScan. The first batch's base offset is spot-checked against the
// segment name so a misplaced file is caught without a full scan.
func restoreFromIndex(path string, base, nextOffset offset.Offset, indexInterval int64) (*segment.Segment, bool, error) {
	size, err := fileSize(path)
	if err != nil {
		return nil, false, err
	}
	if size < codec.HeaderSize || !firstBatchMatches(path, base) {
		return nil, false, nil
	}
	seg, err := segment.Open(path, base, indexInterval)
	if err != nil {
		return nil, false, err
	}
	data, err := os.ReadFile(filesystem.SegmentIndexPath(filepath.Dir(path), base))
	if err != nil {
		_ = seg.Close()
		return nil, false, nil
	}
	ix, err := index.Decode(data, base)
	if err != nil {
		_ = seg.Close()
		return nil, false, nil
	}
	for _, e := range ix.Entries() {
		if int64(e.RelativePosition) >= size {
			_ = seg.Close()
			return nil, false, nil
		}
	}
	if err := seg.RecoverFrom(size, nextOffset, ix.Entries()); err != nil {
		_ = seg.Close()
		return nil, false, err
	}
	return seg, true, nil
}

// scanResult carries a segment restored by scanning plus any torn-tail result.
type scanResult struct {
	seg        *segment.Segment
	truncated  bool
	truncBytes int64
}

// restoreByScan opens a segment and rebuilds its state by scanning the data.
// active controls whether a torn or corrupt tail is truncated (true) or fatal
// (false).
func restoreByScan(path string, base offset.Offset, indexInterval int64, active bool) (*scanResult, error) {
	seg, err := segment.Open(path, base, indexInterval)
	if err != nil {
		return nil, err
	}
	st, err := seg.File().Stat()
	if err != nil {
		_ = seg.Close()
		return nil, err
	}
	size := st.Size()
	next, entries, validSize, torn, err := scan(seg.File(), size, base, indexInterval, active)
	if err != nil {
		_ = seg.Close()
		return nil, err
	}
	sr := &scanResult{seg: seg, truncated: torn}
	if torn {
		if err := seg.TruncateTo(validSize, next); err != nil {
			_ = seg.Close()
			return nil, err
		}
		sr.truncBytes = size - validSize
	}
	if err := seg.RecoverFrom(validSize, next, entries); err != nil {
		_ = seg.Close()
		return nil, err
	}
	return sr, nil
}

// loadPrefixEntries returns the persisted index entries at positions before
// checkpoint. loaded is false when the index file is missing or corrupt.
func loadPrefixEntries(path string, base offset.Offset, checkpoint int64) ([]index.Entry, bool, error) {
	data, err := os.ReadFile(filesystem.SegmentIndexPath(filepath.Dir(path), base))
	if err != nil {
		return nil, false, nil
	}
	ix, err := index.Decode(data, base)
	if err != nil {
		return nil, false, nil
	}
	var out []index.Entry
	for _, e := range ix.Entries() {
		if int64(e.RelativePosition) >= checkpoint {
			break
		}
		out = append(out, e)
	}
	return out, true, nil
}

// fileSize returns the byte length of the file at path.
func fileSize(path string) (int64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// firstBatchMatches reports whether the batch at position 0 of the file
// carries the segment's base offset, catching a segment file that does not
// start where its name claims.
func firstBatchMatches(path string, base offset.Offset) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	var header [codec.HeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return false
	}
	return header[0] == codec.Magic && offset.Offset(binary.BigEndian.Uint64(header[8:16])) == base
}

// scan walks the raw batches of a segment file. For the active segment a torn
// or corrupt tail is reported via torn=true so the caller truncates; for
// sealed segments the same condition is a fatal error.
func scan(f *os.File, size int64, base offset.Offset, indexInterval int64, active bool) (nextOffset offset.Offset, entries []index.Entry, validSize int64, torn bool, err error) {
	return scanFrom(f, 0, size, base, base, indexInterval, active)
}

// scanFrom scans the batches of a segment file starting at the absolute
// position from, expecting the batch at from to carry the base offset expected
// (relativeOffset is always computed against base). It is used to verify only
// the delta tail of an active segment that grew past its snapshot.
func scanFrom(f *os.File, from, size int64, base, expected offset.Offset, indexInterval int64, active bool) (nextOffset offset.Offset, entries []index.Entry, validSize int64, torn bool, err error) {
	if _, err := f.Seek(from, 0); err != nil {
		return 0, nil, 0, false, err
	}
	pos := from
	nextIndexAt := from
	var header [codec.HeaderSize]byte

	for pos < size {
		if size-pos < codec.HeaderSize {
			return truncateOrFatal(active, pos, expected, entries, fmt.Errorf("%w: header overruns file", codec.ErrTruncated))
		}
		if _, err := io.ReadFull(f, header[:]); err != nil {
			return truncateOrFatal(active, pos, expected, entries, err)
		}
		batchLen := binary.BigEndian.Uint32(header[16:20])
		if size-pos-int64(codec.HeaderSize) < int64(batchLen) {
			return truncateOrFatal(active, pos, expected, entries, fmt.Errorf("%w: records overrun file", codec.ErrTruncated))
		}
		records := make([]byte, batchLen)
		if _, err := io.ReadFull(f, records); err != nil {
			return truncateOrFatal(active, pos, expected, entries, err)
		}
		frame := make([]byte, codec.HeaderSize+len(records))
		copy(frame, header[:])
		copy(frame[codec.HeaderSize:], records)

		batch, err := codec.DecodeBatch(frame)
		if err != nil {
			return truncateOrFatal(active, pos, expected, entries, err)
		}
		if batch.BaseOffset != expected {
			return truncateOrFatal(active, pos, expected, entries, fmt.Errorf("%w: base offset %d, want %d", codec.ErrInvalidRecordLength, batch.BaseOffset, expected))
		}
		if pos >= nextIndexAt {
			entries = append(entries, index.Entry{
				RelativeOffset:   uint32(expected - base),
				RelativePosition: uint32(pos),
			})
			nextIndexAt = pos + indexInterval
		}
		total := int64(codec.HeaderSize) + int64(batchLen)
		pos += total
		expected += offset.Offset(len(batch.Records))
	}
	return expected, entries, pos, false, nil
}

// truncateOrFatal turns a corruption into a truncation request for the active
// segment or a fatal error for a sealed one.
func truncateOrFatal(active bool, validSize int64, nextOffset offset.Offset, entries []index.Entry, cause error) (offset.Offset, []index.Entry, int64, bool, error) {
	if active {
		return nextOffset, entries, validSize, true, nil
	}
	return 0, nil, 0, false, cause
}

// trimExt removes the trailing ".log" extension.
func trimExt(name string) string {
	return name[:len(name)-len(".log")]
}
