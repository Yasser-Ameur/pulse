// Package index implements the sparse offset index specified in
// docs/Storage.md §4.
//
// One fixed-size entry (relativeOffset uint32, relativePosition uint32) is
// written per configured interval of appended data. Lookup for an offset
// binary-searches for the greatest entry at or before it, and the caller
// decodes forward from that position. Entries are strictly increasing.
package index

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
)

// EntrySize is the byte length of one index entry.
const EntrySize = 8

// Sentinel errors returned by the index.
var (
	// ErrCorrupt reports an index file that cannot be trusted.
	ErrCorrupt = errors.New("corrupt index")
	// ErrTooLarge reports an entry count that would overflow the 32-bit
	// relative fields.
	ErrTooLarge = errors.New("index entry out of 32-bit range")
)

// Entry is one sparse index entry.
type Entry struct {
	// RelativeOffset is the batch base offset minus the segment base offset.
	RelativeOffset uint32
	// RelativePosition is the batch's byte position within the segment file.
	RelativePosition uint32
}

// Index is a sparse, in-memory offset index for a single segment.
type Index struct {
	baseOffset offset.Offset
	entries    []Entry
}

// New returns an empty index for a segment starting at baseOffset.
func New(baseOffset offset.Offset) *Index {
	return &Index{baseOffset: baseOffset}
}

// BaseOffset returns the segment base offset.
func (ix *Index) BaseOffset() offset.Offset { return ix.baseOffset }

// Len returns the number of entries.
func (ix *Index) Len() int { return len(ix.entries) }

// Entries returns a copy of the entries.
func (ix *Index) Entries() []Entry {
	out := make([]Entry, len(ix.entries))
	copy(out, ix.entries)
	return out
}

// Append records an entry for a batch with the given relative offset and
// position. Entries must be appended in strictly increasing order.
func (ix *Index) Append(relOffset, relPosition uint32) error {
	if n := len(ix.entries); n > 0 {
		last := ix.entries[n-1]
		if relOffset < last.RelativeOffset || (relOffset == last.RelativeOffset && relPosition <= last.RelativePosition) {
			return fmt.Errorf("%w: non-monotonic entry", ErrCorrupt)
		}
	}
	ix.entries = append(ix.entries, Entry{RelativeOffset: relOffset, RelativePosition: relPosition})
	return nil
}

// Lookup returns the file position to start decoding from for a record at the
// absolute offset o, or ok=false when no entry precedes o. The returned entry
// is the greatest with RelativeOffset <= o - baseOffset.
func (ix *Index) Lookup(o offset.Offset) (position int64, ok bool) {
	rel := o - ix.baseOffset
	if rel < 0 {
		return 0, false
	}
	lo, hi := 0, len(ix.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if ix.entries[mid].RelativeOffset <= uint32(rel) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return 0, false
	}
	e := ix.entries[lo-1]
	return int64(e.RelativePosition), true
}

// LastPosition returns the position of the last indexed batch start.
func (ix *Index) LastPosition() (position int64, ok bool) {
	if len(ix.entries) == 0 {
		return 0, false
	}
	return int64(ix.entries[len(ix.entries)-1].RelativePosition), true
}

// TruncateTo drops every entry whose relative offset is at or above relOffset,
// keeping the prefix before relOffset.
func (ix *Index) TruncateTo(relOffset uint32) {
	for n := len(ix.entries); n > 0; n-- {
		if ix.entries[n-1].RelativeOffset < relOffset {
			ix.entries = ix.entries[:n]
			return
		}
	}
	ix.entries = ix.entries[:0]
}

// Encode serializes the entries as big-endian 8-byte records.
func (ix *Index) Encode() []byte {
	out := make([]byte, len(ix.entries)*EntrySize)
	for i, e := range ix.entries {
		binary.BigEndian.PutUint32(out[i*EntrySize:], e.RelativeOffset)
		binary.BigEndian.PutUint32(out[i*EntrySize+4:], e.RelativePosition)
	}
	return out
}

// Decode parses an index file into an index for a segment starting at
// baseOffset. Data with a trailing partial entry is treated as corrupt.
func Decode(data []byte, baseOffset offset.Offset) (*Index, error) {
	if len(data)%EntrySize != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrCorrupt, len(data)%EntrySize)
	}
	ix := New(baseOffset)
	ix.entries = make([]Entry, 0, len(data)/EntrySize)
	for i := 0; i < len(data); i += EntrySize {
		e := Entry{
			RelativeOffset:   binary.BigEndian.Uint32(data[i:]),
			RelativePosition: binary.BigEndian.Uint32(data[i+4:]),
		}
		if n := len(ix.entries); n > 0 {
			last := ix.entries[n-1]
			if e.RelativeOffset < last.RelativeOffset {
				return nil, fmt.Errorf("%w: offsets not increasing", ErrCorrupt)
			}
		}
		ix.entries = append(ix.entries, e)
	}
	return ix, nil
}
