// Package segment manages a single segment file and its sparse offset index.
//
// A segment spans [baseOffset, nextOffset) and holds the record batches
// appended between those offsets. Appends go through the log's writer lock;
// the segment itself is not internally synchronized.
package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/index"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/filesystem"
)

// Segment is one append-only data file plus its in-memory sparse index.
type Segment struct {
	path          string
	indexPath     string
	base          offset.Offset
	file          *os.File
	index         *index.Index
	nextOffset    offset.Offset
	size          int64
	indexInterval int64
	nextIndexAt   int64
	closed        bool
}

// Open opens (creating if needed) the segment data file at path, which must be
// named after base. A freshly created segment starts empty.
func Open(path string, base offset.Offset, indexInterval int64) (*Segment, error) {
	f, err := filesystem.OpenDataFile(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	s := &Segment{
		path:          path,
		indexPath:     filesystem.SegmentIndexPath(filepath.Dir(path), base),
		base:          base,
		file:          f,
		index:         index.New(base),
		nextOffset:    base,
		size:          st.Size(),
		indexInterval: indexInterval,
		nextIndexAt:   0,
	}
	return s, nil
}

// RecoverFrom replaces the segment's runtime state with recovery output: the
// valid data size, the segment LEO, and the rebuilt index entries.
func (s *Segment) RecoverFrom(size int64, nextOffset offset.Offset, entries []index.Entry) error {
	if size < 0 || nextOffset < s.base {
		return fmt.Errorf("%w: invalid recovered state", os.ErrInvalid)
	}
	s.size = size
	s.nextOffset = nextOffset
	s.index = index.New(s.base)
	for _, e := range entries {
		if err := s.index.Append(e.RelativeOffset, e.RelativePosition); err != nil {
			return err
		}
	}
	s.nextIndexAt = size + s.indexInterval
	return nil
}

// Base returns the segment's first offset.
func (s *Segment) Base() offset.Offset { return s.base }

// NextOffset returns the offset the next append in this segment receives.
func (s *Segment) NextOffset() offset.Offset { return s.nextOffset }

// Size returns the number of valid bytes in the data file.
func (s *Segment) Size() int64 { return s.size }

// File exposes the data file for recovery scans.
func (s *Segment) File() *os.File { return s.file }

// Path returns the data file path.
func (s *Segment) Path() string { return s.path }

// LastTimestamp returns the newest record timestamp in the segment, read from
// the last batch. Scanning starts at the last index entry so the cost is
// bounded by one index interval of decode. Batches are time-ordered, so the
// last batch's LastTimestamp is the segment's maximum. Not synchronized; the
// caller must hold the log's writer lock.
func (s *Segment) LastTimestamp() (time.Time, error) {
	from := int64(0)
	if pos, ok := s.index.LastPosition(); ok {
		from = pos
	}
	var last time.Time
	err := s.ScanBatches(from, func(_ int64, _ int64, batch *message.RecordBatch) error {
		last = batch.LastTimestamp
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return last, nil
}

// Append writes an already-encoded batch starting at the segment's current
// LEO and advances the segment state. recordCount is the batch's record count.
func (s *Segment) Append(data []byte, recordCount uint32) error {
	pos := s.size
	if pos >= s.nextIndexAt {
		if err := s.index.Append(uint32(s.nextOffset-s.base), uint32(pos)); err != nil {
			return err
		}
		s.nextIndexAt = pos + s.indexInterval
	}
	if _, err := s.file.WriteAt(data, pos); err != nil {
		return err
	}
	s.size += int64(len(data))
	s.nextOffset += offset.Offset(recordCount)
	return nil
}

// PositionFor returns the file position to start decoding from to reach the
// record at offset o, using the sparse index.
func (s *Segment) PositionFor(o offset.Offset) (int64, bool) {
	return s.index.Lookup(o)
}

// ScanBatches decodes batches sequentially starting at from, calling fn for
// each with its start and end positions. A decode error aborts the scan. It
// uses positional reads so that concurrent readers never contend on the
// shared file offset.
func (s *Segment) ScanBatches(from int64, fn func(pos, end int64, batch *message.RecordBatch) error) error {
	pos := from
	header := make([]byte, codec.HeaderSize)
	for pos < s.size {
		if _, err := s.file.ReadAt(header, pos); err != nil {
			return err
		}
		batchLen := binary.BigEndian.Uint32(header[16:20])
		total := int64(codec.HeaderSize) + int64(batchLen)
		if pos+total > s.size {
			return fmt.Errorf("%w: batch overruns segment", codec.ErrTruncated)
		}
		body := make([]byte, total)
		copy(body, header)
		if _, err := s.file.ReadAt(body[codec.HeaderSize:], pos+int64(codec.HeaderSize)); err != nil {
			return err
		}
		batch, err := codec.DecodeBatch(body)
		if err != nil {
			return err
		}
		if err := fn(pos, pos+total, batch); err != nil {
			return err
		}
		pos += total
	}
	return nil
}

// TruncateTo truncates the data file to validSize and resets the segment LEO
// and index to the state just before the first removed batch.
func (s *Segment) TruncateTo(validSize int64, newNextOffset offset.Offset) error {
	if validSize < 0 || validSize > s.size {
		return fmt.Errorf("%w: invalid truncation size %d", os.ErrInvalid, validSize)
	}
	if err := s.file.Truncate(validSize); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	s.size = validSize
	s.nextOffset = newNextOffset
	s.index.TruncateTo(uint32(newNextOffset - s.base))
	s.nextIndexAt = validSize + s.indexInterval
	return nil
}

// Seal syncs the segment and persists its index without closing the data
// file. Sealed segments stay readable for the life of the log; Close is only
// called on shutdown.
func (s *Segment) Seal() error {
	if s.closed {
		return os.ErrClosed
	}
	return s.Sync()
}

// Sync fsyncs the data file and atomically persists the index.
func (s *Segment) Sync() error {
	if err := s.file.Sync(); err != nil {
		return err
	}
	if s.index.Len() == 0 {
		return nil
	}
	return filesystem.AtomicWriteFile(s.indexPath, s.index.Encode())
}

// Close syncs and closes the segment's files.
func (s *Segment) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	if err := s.Sync(); err != nil {
		errs = append(errs, err)
	}
	if err := s.file.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
