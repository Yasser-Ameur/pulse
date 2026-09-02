package segment

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/index"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/filesystem"
)

var baseTime = time.Unix(1700000000, 0).UTC()

// encodeBatch builds one encoded batch of single-byte payloads starting at base.
func encodeBatch(t *testing.T, base offset.Offset, ts time.Time, payloads ...string) ([]byte, uint32) {
	t.Helper()
	recs := make([]message.Record, len(payloads))
	for i, p := range payloads {
		recs[i] = message.Record{
			Offset:    base + offset.Offset(i),
			Timestamp: ts,
			Message:   message.Message{Payload: []byte(p)},
		}
	}
	data, err := codec.EncodeBatch(&message.RecordBatch{
		BaseOffset:     base,
		FirstTimestamp: ts,
		LastTimestamp:  ts,
		Records:        recs,
	})
	if err != nil {
		t.Fatalf("EncodeBatch() error = %v", err)
	}
	return data, uint32(len(payloads))
}

// openSegment opens a segment for base in a fresh temp dir.
func openSegment(t *testing.T, base offset.Offset, indexInterval int64) (*Segment, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filesystem.SegmentLogPath(dir, base), base, indexInterval)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

// appendBatch appends one batch and returns the position it was written at.
func appendBatch(t *testing.T, s *Segment, ts time.Time, payloads ...string) int64 {
	t.Helper()
	pos := s.Size()
	data, count := encodeBatch(t, s.NextOffset(), ts, payloads...)
	if err := s.Append(data, count); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	return pos
}

func TestOpenCreatesEmptySegment(t *testing.T) {
	s, dir := openSegment(t, 42, 1)

	if s.Base() != 42 {
		t.Errorf("Base() = %v, want 42", s.Base())
	}
	if s.NextOffset() != 42 {
		t.Errorf("NextOffset() = %v, want 42", s.NextOffset())
	}
	if s.Size() != 0 {
		t.Errorf("Size() = %d, want 0", s.Size())
	}
	if s.Path() != filesystem.SegmentLogPath(dir, 42) {
		t.Errorf("Path() = %q, want %q", s.Path(), filesystem.SegmentLogPath(dir, 42))
	}
}

func TestOpenAdoptsExistingFileSize(t *testing.T) {
	dir := t.TempDir()
	path := filesystem.SegmentLogPath(dir, 0)
	if err := os.WriteFile(path, make([]byte, 96), 0o644); err != nil {
		t.Fatalf("seed segment file: %v", err)
	}

	s, err := Open(path, 0, 1)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.Size() != 96 {
		t.Errorf("Size() = %d, want 96 (existing file bytes)", s.Size())
	}
}

func TestOpenFailsOnUnreadablePath(t *testing.T) {
	// A directory where the data file should be: opening it must fail rather
	// than yield a segment backed by nothing.
	dir := t.TempDir()
	path := filesystem.SegmentLogPath(dir, 0)
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}

	if _, err := Open(path, 0, 1); err == nil {
		t.Fatal("Open(directory) error = nil, want error")
	}
}

func TestAppendAdvancesStateAndStaysReadable(t *testing.T) {
	s, _ := openSegment(t, 10, 1)

	appendBatch(t, s, baseTime, "a", "b")
	second := appendBatch(t, s, baseTime, "c")

	if s.NextOffset() != 13 {
		t.Errorf("NextOffset() = %v, want 13", s.NextOffset())
	}
	if s.Size() <= second {
		t.Errorf("Size() = %d, want more than the second batch position %d", s.Size(), second)
	}

	var bases []offset.Offset
	var payloads []string
	if err := s.ScanBatches(0, func(_, _ int64, b *message.RecordBatch) error {
		bases = append(bases, b.BaseOffset)
		for _, r := range b.Records {
			payloads = append(payloads, string(r.Message.Payload))
		}
		return nil
	}); err != nil {
		t.Fatalf("ScanBatches() error = %v", err)
	}
	if len(bases) != 2 || bases[0] != 10 || bases[1] != 12 {
		t.Errorf("batch base offsets = %v, want [10 12]", bases)
	}
	if got := len(payloads); got != 3 {
		t.Errorf("records scanned = %d, want 3", got)
	}
}

func TestAppendIndexesOncePerInterval(t *testing.T) {
	// An interval far larger than a batch indexes only the first batch.
	s, _ := openSegment(t, 0, 1<<20)

	appendBatch(t, s, baseTime, "a")
	appendBatch(t, s, baseTime, "b")
	appendBatch(t, s, baseTime, "c")

	// Every offset resolves to the one entry that precedes it.
	for _, o := range []offset.Offset{0, 1, 2} {
		pos, ok := s.PositionFor(o)
		if !ok || pos != 0 {
			t.Errorf("PositionFor(%d) = (%d, %v), want (0, true)", o, pos, ok)
		}
	}
}

func TestPositionForResolvesEachIndexedBatch(t *testing.T) {
	s, _ := openSegment(t, 0, 1)

	var starts []int64
	for _, p := range []string{"a", "b", "c"} {
		starts = append(starts, appendBatch(t, s, baseTime, p))
	}

	for i, want := range starts {
		pos, ok := s.PositionFor(offset.Offset(i))
		if !ok || pos != want {
			t.Errorf("PositionFor(%d) = (%d, %v), want (%d, true)", i, pos, ok, want)
		}
	}
}

func TestPositionForWithoutIndexEntry(t *testing.T) {
	s, _ := openSegment(t, 100, 1)

	if pos, ok := s.PositionFor(100); ok {
		t.Errorf("PositionFor on empty segment = (%d, true), want ok=false", pos)
	}

	appendBatch(t, s, baseTime, "a")

	if pos, ok := s.PositionFor(50); ok {
		t.Errorf("PositionFor(below base) = (%d, true), want ok=false", pos)
	}
}

func TestScanBatchesStartsAtGivenPosition(t *testing.T) {
	s, _ := openSegment(t, 0, 1)
	appendBatch(t, s, baseTime, "a")
	second := appendBatch(t, s, baseTime, "b")

	var seen []offset.Offset
	if err := s.ScanBatches(second, func(pos, end int64, b *message.RecordBatch) error {
		if pos != second || end != s.Size() {
			t.Errorf("callback pos/end = %d/%d, want %d/%d", pos, end, second, s.Size())
		}
		seen = append(seen, b.BaseOffset)
		return nil
	}); err != nil {
		t.Fatalf("ScanBatches() error = %v", err)
	}
	if len(seen) != 1 || seen[0] != 1 {
		t.Errorf("scanned bases = %v, want [1]", seen)
	}
}

func TestScanBatchesPropagatesCallbackError(t *testing.T) {
	s, _ := openSegment(t, 0, 1)
	appendBatch(t, s, baseTime, "a")
	appendBatch(t, s, baseTime, "b")

	sentinel := errors.New("stop")
	calls := 0
	err := s.ScanBatches(0, func(int64, int64, *message.RecordBatch) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("ScanBatches() error = %v, want %v", err, sentinel)
	}
	if calls != 1 {
		t.Errorf("callback called %d times, want 1 (scan must stop on error)", calls)
	}
}

func TestScanBatchesRejectsBatchOverrunningSegment(t *testing.T) {
	s, _ := openSegment(t, 0, 1)
	appendBatch(t, s, baseTime, "a")
	second := appendBatch(t, s, baseTime, "b")

	// Recovery decided fewer bytes are valid than the second batch needs.
	if err := s.RecoverFrom(second+codec.HeaderSize+2, 1, nil); err != nil {
		t.Fatalf("RecoverFrom() error = %v", err)
	}

	if err := s.ScanBatches(0, func(int64, int64, *message.RecordBatch) error { return nil }); !errors.Is(err, codec.ErrTruncated) {
		t.Fatalf("ScanBatches() error = %v, want ErrTruncated", err)
	}
}

func TestTruncateToRejectsSizeOutOfRange(t *testing.T) {
	s, _ := openSegment(t, 0, 1)
	appendBatch(t, s, baseTime, "a")
	size := s.Size()

	tests := map[string]int64{
		"negative":    -1,
		"beyond size": size + 1,
	}
	for name, validSize := range tests {
		t.Run(name, func(t *testing.T) {
			if err := s.TruncateTo(validSize, 0); !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("TruncateTo(%d) error = %v, want os.ErrInvalid", validSize, err)
			}
			if s.Size() != size {
				t.Errorf("Size() = %d, want %d (rejected truncation must not mutate)", s.Size(), size)
			}
		})
	}
}

func TestTruncateToShrinksFileAndDropsIndexEntries(t *testing.T) {
	s, dir := openSegment(t, 0, 1)
	appendBatch(t, s, baseTime, "a")
	second := appendBatch(t, s, baseTime, "b")
	appendBatch(t, s, baseTime, "c")

	if err := s.TruncateTo(second, 1); err != nil {
		t.Fatalf("TruncateTo() error = %v", err)
	}

	if s.Size() != second {
		t.Errorf("Size() = %d, want %d", s.Size(), second)
	}
	if s.NextOffset() != 1 {
		t.Errorf("NextOffset() = %v, want 1", s.NextOffset())
	}
	st, err := os.Stat(filesystem.SegmentLogPath(dir, 0))
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	if st.Size() != second {
		t.Errorf("on-disk size = %d, want %d", st.Size(), second)
	}
	if pos, ok := s.PositionFor(0); !ok || pos != 0 {
		t.Errorf("PositionFor(0) = (%d, %v), want (0, true): surviving entry dropped", pos, ok)
	}
	if pos, ok := s.PositionFor(1); ok && pos >= second {
		t.Errorf("PositionFor(1) = (%d, true), want a position below %d", pos, second)
	}
}

func TestRecoverFromRejectsInvalidState(t *testing.T) {
	s, _ := openSegment(t, 10, 1)

	tests := map[string]struct {
		size int64
		next offset.Offset
	}{
		"negative size":  {size: -1, next: 10},
		"leo below base": {size: 0, next: 9},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if err := s.RecoverFrom(tc.size, tc.next, nil); !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("RecoverFrom(%d, %v) error = %v, want os.ErrInvalid", tc.size, tc.next, err)
			}
		})
	}
}

func TestRecoverFromRejectsNonMonotonicEntries(t *testing.T) {
	s, _ := openSegment(t, 0, 1)

	entries := []index.Entry{
		{RelativeOffset: 5, RelativePosition: 100},
		{RelativeOffset: 2, RelativePosition: 200},
	}
	if err := s.RecoverFrom(300, 8, entries); !errors.Is(err, index.ErrCorrupt) {
		t.Fatalf("RecoverFrom(non-monotonic) error = %v, want index.ErrCorrupt", err)
	}
}

func TestRecoverFromReplacesStateAndIndex(t *testing.T) {
	s, _ := openSegment(t, 0, 1)
	appendBatch(t, s, baseTime, "a")

	entries := []index.Entry{
		{RelativeOffset: 0, RelativePosition: 0},
		{RelativeOffset: 4, RelativePosition: 128},
	}
	if err := s.RecoverFrom(256, 8, entries); err != nil {
		t.Fatalf("RecoverFrom() error = %v", err)
	}

	if s.Size() != 256 || s.NextOffset() != 8 {
		t.Errorf("Size()/NextOffset() = %d/%v, want 256/8", s.Size(), s.NextOffset())
	}
	if pos, ok := s.PositionFor(5); !ok || pos != 128 {
		t.Errorf("PositionFor(5) = (%d, %v), want (128, true)", pos, ok)
	}
}

func TestLastTimestampOnEmptySegment(t *testing.T) {
	s, _ := openSegment(t, 0, 1)

	ts, err := s.LastTimestamp()
	if err != nil {
		t.Fatalf("LastTimestamp() error = %v", err)
	}
	if !ts.IsZero() {
		t.Errorf("LastTimestamp() = %v, want zero time", ts)
	}
}

func TestLastTimestampReturnsNewestBatch(t *testing.T) {
	s, _ := openSegment(t, 0, 1)
	newest := baseTime.Add(90 * time.Minute)
	appendBatch(t, s, baseTime, "a")
	appendBatch(t, s, baseTime.Add(time.Hour), "b")
	appendBatch(t, s, newest, "c")

	ts, err := s.LastTimestamp()
	if err != nil {
		t.Fatalf("LastTimestamp() error = %v", err)
	}
	if !ts.Equal(newest) {
		t.Errorf("LastTimestamp() = %v, want %v", ts, newest)
	}
}

func TestSealPersistsIndexFile(t *testing.T) {
	s, dir := openSegment(t, 7, 1)
	appendBatch(t, s, baseTime, "a")
	second := appendBatch(t, s, baseTime, "b")

	if err := s.Seal(); err != nil {
		t.Fatalf("Seal() error = %v", err)
	}

	data, err := os.ReadFile(filesystem.SegmentIndexPath(dir, 7))
	if err != nil {
		t.Fatalf("read persisted index: %v", err)
	}
	ix, err := index.Decode(data, 7)
	if err != nil {
		t.Fatalf("index.Decode() error = %v", err)
	}
	if pos, ok := ix.Lookup(8); !ok || pos != second {
		t.Errorf("persisted Lookup(8) = (%d, %v), want (%d, true)", pos, ok, second)
	}
}

func TestSyncWithEmptyIndexWritesNoIndexFile(t *testing.T) {
	s, dir := openSegment(t, 0, 1)

	if err := s.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if _, err := os.Stat(filesystem.SegmentIndexPath(dir, 0)); !os.IsNotExist(err) {
		t.Errorf("index file exists for an empty index (stat err = %v)", err)
	}
}

func TestSealAfterCloseFails(t *testing.T) {
	s, _ := openSegment(t, 0, 1)
	appendBatch(t, s, baseTime, "a")
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := s.Seal(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Seal() after Close error = %v, want os.ErrClosed", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	s, _ := openSegment(t, 0, 1)
	appendBatch(t, s, baseTime, "a")

	if err := s.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
}
