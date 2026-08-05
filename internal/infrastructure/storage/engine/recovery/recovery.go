// Package recovery rebuilds a partition's log state at startup.
//
// Every segment is scanned batch by batch: batch frames are decoded (magic,
// version, length, continuity, CRC) and the sparse index is rebuilt from the
// data. A torn or corrupted tail on the active (last) segment is truncated at
// the previous valid batch boundary; any corruption in a sealed segment is
// fatal, because only the log tail can legitimately be damaged by a crash.
// See docs/Storage.md §7.
package recovery

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/index"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/segment"
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
}

// Run recovers every segment in the partition directory dir. An empty
// directory yields no segments.
func Run(dir string, indexInterval int64) (*Result, error) {
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
		st, err := seg.File().Stat()
		if err != nil {
			_ = seg.Close()
			return nil, err
		}
		size := st.Size()
		nextOffset, entries, validSize, torn, err := scan(seg.File(), size, base, indexInterval, last)
		if err != nil {
			_ = seg.Close()
			return nil, fmt.Errorf("recover %s: %w", path, err)
		}
		if torn {
			res.Truncated = true
			res.TruncatedBytes += size - validSize
			if err := seg.TruncateTo(validSize, nextOffset); err != nil {
				_ = seg.Close()
				return nil, fmt.Errorf("recover %s: %w", path, err)
			}
		} else if err := seg.RecoverFrom(validSize, nextOffset, entries); err != nil {
			_ = seg.Close()
			return nil, fmt.Errorf("recover %s: %w", path, err)
		}
		res.Segments = append(res.Segments, seg)
	}
	return res, nil
}

// scan walks the raw batches of a segment file. For the active segment a torn
// or corrupt tail is reported via torn=true so the caller truncates; for
// sealed segments the same condition is a fatal error.
func scan(f *os.File, size int64, base offset.Offset, indexInterval int64, active bool) (nextOffset offset.Offset, entries []index.Entry, validSize int64, torn bool, err error) {
	pos := int64(0)
	expected := base
	nextIndexAt := int64(0)
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
