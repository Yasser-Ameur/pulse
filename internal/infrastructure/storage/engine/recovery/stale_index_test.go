package recovery

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/index"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/segment"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/snapshot"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/filesystem"
)

// These tests drive runWithSnapshot against index files that disagree with the
// data they describe. The snapshot fast path skips the batch-by-batch scan that
// is recovery's correctness baseline, so whatever it trusts is only as good as
// the checks it makes before trusting it.

// staleIdxWriteEntries writes an index file for base holding exactly the given
// entries, bypassing the batch positions the data actually has.
//
// It builds the file through index.Append rather than assembling bytes, so an
// entry that reaches disk here is one the index package itself accepted.
func staleIdxWriteEntries(t *testing.T, dir string, base offset.Offset, entries []index.Entry) {
	t.Helper()
	ix := index.New(base)
	for _, e := range entries {
		if err := ix.Append(e.RelativeOffset, e.RelativePosition); err != nil {
			t.Fatalf("index.Append(%d, %d) error = %v", e.RelativeOffset, e.RelativePosition, err)
		}
	}
	if err := os.WriteFile(filesystem.SegmentIndexPath(dir, base), ix.Encode(), 0o644); err != nil {
		t.Fatalf("write index file: %v", err)
	}
}

// staleIdxWriteRawEntries writes an index file for base straight from bytes,
// for entries index.Append would refuse to produce.
func staleIdxWriteRawEntries(t *testing.T, dir string, base offset.Offset, entries []index.Entry) {
	t.Helper()
	buf := make([]byte, len(entries)*index.EntrySize)
	for i, e := range entries {
		binary.BigEndian.PutUint32(buf[i*index.EntrySize:], e.RelativeOffset)
		binary.BigEndian.PutUint32(buf[i*index.EntrySize+4:], e.RelativePosition)
	}
	if err := os.WriteFile(filesystem.SegmentIndexPath(dir, base), buf, 0o644); err != nil {
		t.Fatalf("write index file: %v", err)
	}
}

// staleIdxTruncateFile cuts the segment data file down to size, the state a
// crash leaves when the tail never reached the disk.
func staleIdxTruncateFile(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("truncate %s to %d: %v", path, size, err)
	}
}

// staleIdxScanFrom returns every record offset a reader sees when it starts
// decoding at pos and walks to the end of the segment.
func staleIdxScanFrom(t *testing.T, seg *segment.Segment, pos int64) []offset.Offset {
	t.Helper()
	var seen []offset.Offset
	err := seg.ScanBatches(pos, func(_ int64, _ int64, batch *message.RecordBatch) error {
		for i := range batch.Records {
			seen = append(seen, batch.Records[i].Offset)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ScanBatches(%d) error = %v", pos, err)
	}
	return seen
}

// staleIdxLookupAndScan resolves o through the sparse index the way Log.Read
// does, starting at the indexed position or at zero when no entry precedes o,
// and returns the offsets the resulting scan yields.
func staleIdxLookupAndScan(t *testing.T, seg *segment.Segment, o offset.Offset) (int64, []offset.Offset) {
	t.Helper()
	pos := int64(0)
	if p, ok := seg.PositionFor(o); ok {
		pos = p
	}
	return pos, staleIdxScanFrom(t, seg, pos)
}

func staleIdxContains(offsets []offset.Offset, o offset.Offset) bool {
	for _, got := range offsets {
		if got == o {
			return true
		}
	}
	return false
}

// TestStaleIndexShorterThanDataResolvesEveryOffset covers the stale index a
// crash actually produces.
//
// A segment fsyncs its data on every acknowledged append but rewrites the index
// file only at seal, close, an explicit sync, or the interval tick. So the
// index on disk is routinely a short prefix of the data, and a crash freezes it
// that way. The snapshot still matches the file exactly, so runWithSnapshot
// takes the trust-the-tail branch and restores from that short index.
//
// The claim under test is the one segment.SyncData makes: a shorter index is a
// valid prefix that only costs a longer forward scan. Every record must still
// be reachable.
func TestStaleIndexShorterThanDataResolvesEveryOffset(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"}, []string{"c"})
	// Only the first batch made it into the index file.
	writeIndexFile(t, dir, s, 1)
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.next, ActiveBase: s.base, ActiveSize: s.size, ActiveNext: s.next,
	})

	res := mustRun(t, dir, testIndexInterval)

	if !res.SnapshotUsed {
		t.Fatal("SnapshotUsed = false, want true")
	}
	if res.Truncated {
		t.Errorf("Truncated = true, want false: the data was never torn")
	}
	if len(res.Segments) != 1 {
		t.Fatalf("Segments = %d, want 1", len(res.Segments))
	}
	seg := res.Segments[0]
	if seg.NextOffset() != s.next {
		t.Errorf("NextOffset() = %v, want %v", seg.NextOffset(), s.next)
	}
	if seg.Size() != s.size {
		t.Errorf("Size() = %d, want %d", seg.Size(), s.size)
	}

	// Size() must land on a batch boundary, or the next append writes over the
	// tail of a committed batch. A full scan that consumes the file without a
	// truncation error is what proves it.
	if got := staleIdxScanFrom(t, seg, 0); len(got) != len(s.bases) {
		t.Errorf("full scan yielded %d records, want %d", len(got), len(s.bases))
	}

	// Every batch base is reachable through the index even though only the
	// first has an entry.
	for i, base := range s.bases {
		pos, seen := staleIdxLookupAndScan(t, seg, base)
		if !staleIdxContains(seen, base) {
			t.Errorf("offset %v (batch %d): scan from indexed position %d yielded %v, offset not reachable",
				base, i, pos, seen)
		}
	}
}

// TestStaleIndexPastEndOfDataFallsBackToScan covers the one crash window that
// leaves the index describing more than the data holds.
//
// Segment.TruncateTo truncates and fsyncs the data file before it persists the
// shortened index. A crash in between leaves the longer pre-truncation index
// next to the shortened data, so its trailing entries point at bytes that are
// gone. runWithSnapshot must notice and rescan rather than hand back a segment
// whose index addresses a hole.
func TestStaleIndexPastEndOfDataFallsBackToScan(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"}, []string{"c"})
	// The index that was on disk before the truncation: all three batches.
	writeIndexFile(t, dir, s, len(s.starts))
	// The truncation the crash interrupted: the third batch is gone.
	staleIdxTruncateFile(t, s.path, s.starts[2])
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.bases[2], ActiveBase: s.base, ActiveSize: s.starts[2], ActiveNext: s.bases[2],
	})

	res := mustRun(t, dir, testIndexInterval)

	if !res.SnapshotUsed {
		t.Fatal("SnapshotUsed = false, want true")
	}
	if len(res.Segments) != 1 {
		t.Fatalf("Segments = %d, want 1", len(res.Segments))
	}
	seg := res.Segments[0]
	if seg.NextOffset() != s.bases[2] {
		t.Errorf("NextOffset() = %v, want %v", seg.NextOffset(), s.bases[2])
	}
	if seg.Size() != s.starts[2] {
		t.Errorf("Size() = %d, want %d", seg.Size(), s.starts[2])
	}
	// The stale index was discarded and rebuilt from the surviving data, so the
	// entry pointing past the end is gone.
	assertIndexed(t, seg, s, 2)
	if pos, ok := seg.PositionFor(s.bases[2]); ok && pos >= seg.Size() {
		t.Errorf("PositionFor(%v) = %d, still past Size() = %d", s.bases[2], pos, seg.Size())
	}
}

// TestStaleIndexEntryAheadOfItsBatchIsTrustedAndHidesRecords is the gap.
//
// restoreFromIndex validates a loaded index with exactly two checks: the first
// batch's base offset matches the segment name, and every entry's position is
// below the file size. Nothing checks that an entry's position is where its
// offset actually lives. An entry that points past its own batch therefore
// passes, and MarkIndexPersisted then tells the segment the file is current, so
// the wrong index is never rewritten.
//
// The harm is not a decode failure. Log.Read starts at the indexed position and
// drops records below the requested offset, so an entry pointing too far
// forward makes the requested record silently invisible: the read returns later
// records and no error. Compare the data file, where every batch carries a CRC
// that recovery verifies; the index file has no checksum, and this is the only
// validation it gets.
//
// Two things let the bad entry through. index.Decode rejects only a decreasing
// RelativeOffset, and index.Append compares positions only when two entries
// share an offset, so neither enforces the strictly-increasing invariant the
// index package documents. The entry below is built through index.Append to
// show it is accepted there, not merely tolerated on the way in.
//
// This is not reachable by killing the process: persistIndex goes through
// filesystem.AtomicWriteFile, so the file on disk is always some complete,
// self-consistent encoding. It is reachable by anything that changes the bytes
// underneath, such as bit rot, a bad sector, or an index restored from backup
// beside a newer segment.
func TestStaleIndexEntryAheadOfItsBatchIsTrustedAndHidesRecords(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"}, []string{"c"})
	// The entry for the second batch points at the third batch's start. Offsets
	// increase, positions increase, and both are inside the file, so every
	// check between here and the recovered segment passes.
	staleIdxWriteEntries(t, dir, s.base, []index.Entry{
		{RelativeOffset: uint32(s.bases[0] - s.base), RelativePosition: uint32(s.starts[0])},
		{RelativeOffset: uint32(s.bases[1] - s.base), RelativePosition: uint32(s.starts[2])},
	})
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.next, ActiveBase: s.base, ActiveSize: s.size, ActiveNext: s.next,
	})

	res := mustRun(t, dir, testIndexInterval)

	if !res.SnapshotUsed {
		t.Fatal("SnapshotUsed = false, want true")
	}
	seg := res.Segments[0]

	// The data itself is intact: a scan from zero still sees every record.
	if got := staleIdxScanFrom(t, seg, 0); len(got) != len(s.bases) {
		t.Fatalf("full scan yielded %d records, want %d", len(got), len(s.bases))
	}

	pos, seen := staleIdxLookupAndScan(t, seg, s.bases[1])
	if pos != s.starts[2] {
		t.Fatalf("PositionFor(%v) = %d, want the planted %d", s.bases[1], pos, s.starts[2])
	}
	if staleIdxContains(seen, s.bases[1]) {
		t.Fatalf("offset %v was reachable from the planted position %d; the gap this test "+
			"documents is closed, so replace it with the assertion that recovery rejected "+
			"the index", s.bases[1], pos)
	}
	// Recovery reported a healthy segment and no truncation, yet a read of
	// s.bases[1] now starts past it and returns only later records.
	t.Logf("recovery accepted an index entry that points past its own batch: "+
		"Read(%v) would start at byte %d and yield %v, silently skipping %v",
		s.bases[1], pos, seen, s.bases[1])
}

// TestStaleIndexRejectedByAppendIsFatalInsteadOfRescanned is the second gap,
// and it is the opposite failure: recovery refuses to start rather than
// trusting too much.
//
// The index package validates in two places that do not agree. index.Decode
// rejects only a decreasing RelativeOffset, while index.Append also rejects two
// entries that share an offset without advancing the position. A file in that
// gap decodes, so restoreFromIndex accepts it and hands it to
// Segment.RecoverFrom, whose per-entry Append then fails.
//
// restoreFromIndex returns that failure as err rather than as restored=false.
// Its own doc comment says restored is false when the index is missing or
// corrupt so the caller falls back to restoreByScan; docs/Storage.md §8 step 2
// says a missing or corrupt index falls back to scanning that segment, and §7
// step 2 says the index is validated for monotonic offsets and rebuilt from the
// data when it does not hold up. Neither happens: Run propagates the error and
// the partition does not open,
// even though the data file is intact and a scan would rebuild the index from
// it. The index is a cache, so this turns a recoverable cache fault into an
// unavailable partition.
//
// The same shape sits on the delta branch, where loadPrefixEntries filters by
// position and RecoverFrom appends prefix and scanned tail together.
func TestStaleIndexRejectedByAppendIsFatalInsteadOfRescanned(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"}, []string{"c"})
	// Offsets never decrease, so Decode accepts the file; the repeated offset
	// at a position that does not advance is what Append rejects.
	staleIdxWriteRawEntries(t, dir, s.base, []index.Entry{
		{RelativeOffset: uint32(s.bases[0] - s.base), RelativePosition: uint32(s.starts[0])},
		{RelativeOffset: uint32(s.bases[1] - s.base), RelativePosition: uint32(s.starts[1])},
		{RelativeOffset: uint32(s.bases[1] - s.base), RelativePosition: uint32(s.starts[1])},
	})
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.next, ActiveBase: s.base, ActiveSize: s.size, ActiveNext: s.next,
	})

	res, err := Run(dir, testIndexInterval)
	if err == nil {
		for _, seg := range res.Segments {
			_ = seg.Close()
		}
		t.Fatal("Run() error = nil; recovery now falls back to the scan, so this test " +
			"should assert the recovered state instead of the failure")
	}
	if !errors.Is(err, index.ErrCorrupt) {
		t.Fatalf("Run() error = %v, want index.ErrCorrupt", err)
	}
	if res != nil {
		t.Errorf("Run() returned a Result alongside the error")
	}

	// The data is fine: deleting the index file alone lets recovery rebuild it.
	if err := os.Remove(filesystem.SegmentIndexPath(dir, s.base)); err != nil {
		t.Fatalf("remove index file: %v", err)
	}
	recovered := mustRun(t, dir, testIndexInterval)
	if recovered.Segments[0].NextOffset() != s.next {
		t.Fatalf("after removing the index, NextOffset() = %v, want %v",
			recovered.Segments[0].NextOffset(), s.next)
	}
	t.Log("a corrupt index file made Run fail; the same directory recovers cleanly " +
		"once the index is deleted, so the scan fallback would have worked")
}

// TestStaleIndexShorterThanCheckpointOnDeltaBranch is the same lagging index as
// the first test, but on the other half of runWithSnapshot.
//
// Under interval sync mode the index falls behind the data by design, and the
// log keeps appending after the snapshot is written. Recovery then takes the
// delta branch: loadPrefixEntries keeps the persisted entries below the
// checkpoint and scanFrom rebuilds the tail above it, and RecoverFrom appends
// the two together. A short prefix leaves a hole in the middle of the index,
// which must still cost nothing but a longer forward scan.
func TestStaleIndexShorterThanCheckpointOnDeltaBranch(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"}, []string{"c"})
	// The index stopped at the first batch, but the checkpoint covers two and
	// the third was appended after it.
	writeIndexFile(t, dir, s, 1)
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.bases[2], ActiveBase: s.base, ActiveSize: s.starts[2], ActiveNext: s.bases[2],
	})

	res := mustRun(t, dir, testIndexInterval)

	if !res.SnapshotUsed || res.Truncated {
		t.Fatalf("Result = %+v, want SnapshotUsed without truncation", *res)
	}
	seg := res.Segments[0]
	if seg.NextOffset() != s.next {
		t.Errorf("NextOffset() = %v, want %v", seg.NextOffset(), s.next)
	}
	if seg.Size() != s.size {
		t.Errorf("Size() = %d, want %d", seg.Size(), s.size)
	}
	// The middle batch has no entry of its own, so its lookup lands on the
	// first batch and the scan walks to it.
	if pos, ok := seg.PositionFor(s.bases[1]); !ok || pos != s.starts[0] {
		t.Errorf("PositionFor(%v) = (%d, %v), want (%d, true)", s.bases[1], pos, ok, s.starts[0])
	}
	for i, base := range s.bases {
		pos, seen := staleIdxLookupAndScan(t, seg, base)
		if !staleIdxContains(seen, base) {
			t.Errorf("offset %v (batch %d): scan from indexed position %d yielded %v, offset not reachable",
				base, i, pos, seen)
		}
	}
}
