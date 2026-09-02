package segment

import (
	"os"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/index"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/filesystem"
)

// hotpathPayload is the message size the append path was measured with.
const hotpathPayload = 256

// hotpathBatch encodes the one-record batch that Log.Append hands to
// Segment.Append.
func hotpathBatch(tb testing.TB) []byte {
	tb.Helper()
	ts := time.Unix(1700000000, 0).UTC()
	data, err := codec.EncodeBatch(&message.RecordBatch{
		BaseOffset:     0,
		FirstTimestamp: ts,
		LastTimestamp:  ts,
		Records: []message.Record{{
			Offset:    0,
			Timestamp: ts,
			Message:   message.Message{Payload: make([]byte, hotpathPayload)},
		}},
	})
	if err != nil {
		tb.Fatalf("EncodeBatch() error = %v", err)
	}
	return data
}

// hotpathSegment opens an empty segment in a fresh directory and returns it
// with that directory.
func hotpathSegment(tb testing.TB, indexInterval int64) (*Segment, string) {
	tb.Helper()
	dir := tb.TempDir()
	seg, err := Open(filesystem.SegmentLogPath(dir, 0), 0, indexInterval)
	if err != nil {
		tb.Fatalf("Open() error = %v", err)
	}
	tb.Cleanup(func() { _ = seg.Close() })
	return seg, dir
}

// hotpathAppend appends one encoded batch.
func hotpathAppend(tb testing.TB, s *Segment, data []byte) {
	tb.Helper()
	if err := s.Append(data, 1); err != nil {
		tb.Fatalf("Append() error = %v", err)
	}
}

// hotpathEntries decodes the persisted index file and returns its entry count.
func hotpathEntries(tb testing.TB, path string) int {
	tb.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("read index file: %v", err)
	}
	ix, err := index.Decode(raw, 0)
	if err != nil {
		tb.Fatalf("index.Decode() error = %v", err)
	}
	return ix.Len()
}

// TestSyncDataLeavesIndexFileUntouched pins the append hot path: a durable
// append fsyncs the data file and nothing else. The sparse index is a cache
// the recovery scan rebuilds, so it is not part of an append's durability.
func TestSyncDataLeavesIndexFileUntouched(t *testing.T) {
	seg, dir := hotpathSegment(t, 4096)
	data := hotpathBatch(t)
	for i := 0; i < 8; i++ {
		hotpathAppend(t, seg, data)
		if err := seg.SyncData(); err != nil {
			t.Fatalf("SyncData() error = %v", err)
		}
	}
	if _, err := os.Stat(filesystem.SegmentIndexPath(dir, 0)); !os.IsNotExist(err) {
		t.Fatalf("SyncData() touched the index file; the hot path must fsync the data file only (stat err = %v)", err)
	}
}

// TestSyncSkipsUnchangedIndex pins the other half: Sync persists the index
// only when an entry was actually appended to it. Rewriting a byte-identical
// index costs a temp-file create, an fsync, a rename and a directory fsync.
//
// The check is deterministic rather than timestamp-based: the persisted file
// is deleted, and AtomicWriteFile would recreate it, so its continued absence
// proves no write happened.
func TestSyncSkipsUnchangedIndex(t *testing.T) {
	data := hotpathBatch(t)
	// Eight batches per index interval: the first append is indexed, the next
	// seven are not, and the ninth is.
	seg, dir := hotpathSegment(t, int64(len(data))*8)
	indexPath := filesystem.SegmentIndexPath(dir, 0)

	hotpathAppend(t, seg, data)
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if got := hotpathEntries(t, indexPath); got != 1 {
		t.Fatalf("persisted index entries after first Sync() = %d, want 1", got)
	}
	if err := os.Remove(indexPath); err != nil {
		t.Fatalf("remove index file: %v", err)
	}

	for i := 1; i < 8; i++ {
		hotpathAppend(t, seg, data)
		if err := seg.Sync(); err != nil {
			t.Fatalf("Sync() error = %v", err)
		}
	}
	if got := seg.index.Len(); got != 1 {
		t.Fatalf("in-memory index entries = %d, want 1", got)
	}
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		t.Fatalf("Sync() rewrote a byte-identical index (stat err = %v)", err)
	}

	// The ninth append crosses the interval, so the new entry must be persisted.
	hotpathAppend(t, seg, data)
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if got := hotpathEntries(t, indexPath); got != 2 {
		t.Fatalf("persisted index entries after a new entry = %d, want 2", got)
	}
}

// TestSealPersistsIndexAfterHotPathSyncs covers the rotation crux: a segment
// that only ever saw data-only syncs must still leave a complete index behind
// when the log rotates away from it, or a sealed segment keeps a permanently
// stale index on disk.
func TestSealPersistsIndexAfterHotPathSyncs(t *testing.T) {
	data := hotpathBatch(t)
	seg, dir := hotpathSegment(t, int64(len(data)))
	for i := 0; i < 8; i++ {
		hotpathAppend(t, seg, data)
		if err := seg.SyncData(); err != nil {
			t.Fatalf("SyncData() error = %v", err)
		}
	}
	if err := seg.Seal(); err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	got := hotpathEntries(t, filesystem.SegmentIndexPath(dir, 0))
	if want := seg.index.Len(); got != want {
		t.Fatalf("persisted index entries after Seal() = %d, want %d", got, want)
	}
}

// TestTruncateToPersistsIndex covers the one case where an index entry count
// can shrink: the file on disk must never keep entries pointing at positions
// the truncation released for reuse.
func TestTruncateToPersistsIndex(t *testing.T) {
	data := hotpathBatch(t)
	seg, dir := hotpathSegment(t, int64(len(data)))
	for i := 0; i < 8; i++ {
		hotpathAppend(t, seg, data)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if err := seg.TruncateTo(int64(len(data))*3, 3); err != nil {
		t.Fatalf("TruncateTo() error = %v", err)
	}
	if got := hotpathEntries(t, filesystem.SegmentIndexPath(dir, 0)); got != 3 {
		t.Fatalf("persisted index entries after TruncateTo() = %d, want 3", got)
	}
}

// TestScanRecoveredSegmentPersistsIndexOnClose pins the seeding contract of
// RecoverFrom. Recovery rebuilds an index by scanning precisely when the index
// file could not be used, so at that moment the file does not hold what memory
// holds. A segment that recorded those rebuilt entries as already persisted
// would skip the write on the clean shutdown that is supposed to heal the
// file, and a sealed segment never gets the later append that would force one.
func TestScanRecoveredSegmentPersistsIndexOnClose(t *testing.T) {
	data := hotpathBatch(t)
	seg, dir := hotpathSegment(t, int64(len(data)))
	for i := 0; i < 4; i++ {
		hotpathAppend(t, seg, data)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	scanned := seg.index.Entries()
	size, next := seg.Size(), seg.NextOffset()
	if len(scanned) == 0 {
		t.Fatal("test needs a non-empty rebuilt index")
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Replay restoreByScan: the index file is gone, so recovery opens the
	// segment and hands RecoverFrom the entries it rebuilt from the data.
	indexPath := filesystem.SegmentIndexPath(dir, 0)
	if err := os.Remove(indexPath); err != nil {
		t.Fatalf("remove index file: %v", err)
	}
	reopened, err := Open(filesystem.SegmentLogPath(dir, 0), 0, int64(len(data)))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := reopened.RecoverFrom(size, next, scanned); err != nil {
		t.Fatalf("RecoverFrom() error = %v", err)
	}
	// Shut down with no append in between, as a sealed segment always does.
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := hotpathEntries(t, indexPath); got != len(scanned) {
		t.Fatalf("persisted index entries after scan recovery = %d, want %d", got, len(scanned))
	}
}

// BenchmarkAppendDurableSync measures one durable append on the every-write
// path: the segment write plus the fsync Log.Append performs for each batch.
// Encoding is hoisted out of the loop so the durability cost is what varies.
func BenchmarkAppendDurableSync(b *testing.B) {
	seg, _ := hotpathSegment(b, 4096)
	data := hotpathBatch(b)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := seg.Append(data, 1); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
		if err := seg.SyncData(); err != nil {
			b.Fatalf("SyncData() error = %v", err)
		}
	}
}
