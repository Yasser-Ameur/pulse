package recovery

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/filesystem"
)

// writeSparseSegment writes a single-batch segment file named after base whose
// sole record sits at recordOffset, simulating a sealed segment that
// compaction has reduced to one survivor. recordOffset need not equal base: a
// compacted segment may no longer start with its own base offset.
func writeSparseSegment(t *testing.T, dir string, base, recordOffset offset.Offset, payload string) {
	t.Helper()
	ts := time.Unix(1700000000, 0).UTC()
	data, err := codec.EncodeBatch(&message.RecordBatch{
		BaseOffset:     recordOffset,
		FirstTimestamp: ts,
		LastTimestamp:  ts,
		Records: []message.Record{{
			Offset:    recordOffset,
			Timestamp: ts,
			Message:   message.Message{Payload: []byte(payload)},
		}},
	})
	if err != nil {
		t.Fatalf("EncodeBatch() error = %v", err)
	}
	if err := os.WriteFile(filesystem.SegmentLogPath(dir, base), data, 0o644); err != nil {
		t.Fatalf("write segment %s: %v", filesystem.SegmentLogPath(dir, base), err)
	}
}

// TestSparseSealedSegmentFullScanRecovery covers a full-scan recovery (no
// snapshot) over a sealed segment that compaction reduced to a sparse subset
// of its original offset range. The segment's LEO must come from the next
// segment's base offset, not from its own surviving data.
func TestSparseSealedSegmentFullScanRecovery(t *testing.T) {
	dir := t.TempDir()
	// Segment 0 originally spanned [0,3): compaction dropped offsets 0 and 2,
	// leaving only the record at offset 1. A naive "last record + 1" would
	// derive a LEO of 2; the true LEO, 3, comes only from the next segment's
	// file name.
	writeSparseSegment(t, dir, 0, 1, "kept")
	buildSegment(t, dir, 3, []string{"tail"})

	res := mustRun(t, dir, testIndexInterval)
	if len(res.Segments) != 2 {
		t.Fatalf("Segments = %d, want 2", len(res.Segments))
	}
	sealed := res.Segments[0]
	if sealed.Base() != 0 {
		t.Errorf("sealed Base() = %v, want 0", sealed.Base())
	}
	if sealed.NextOffset() != 3 {
		t.Errorf("sealed NextOffset() = %v, want 3 (derived from the next segment's base, not its own data)", sealed.NextOffset())
	}
	if res.Truncated {
		t.Errorf("Truncated = true, want false: the sparse data is not corruption")
	}

	// The hole is visible: offset 1 is reachable, offsets 0 and 2 are not
	// present in the segment's data at all.
	var seen []offset.Offset
	if err := sealed.ScanBatches(0, func(_, _ int64, batch *message.RecordBatch) error {
		for i := range batch.Records {
			seen = append(seen, batch.Records[i].Offset)
		}
		return nil
	}); err != nil {
		t.Fatalf("ScanBatches() error = %v", err)
	}
	if len(seen) != 1 || seen[0] != 1 {
		t.Fatalf("scanned offsets = %v, want [1]", seen)
	}

	active := res.Segments[1]
	if active.Base() != 3 || active.NextOffset() != 4 {
		t.Errorf("active segment = base %v next %v, want base 3 next 4", active.Base(), active.NextOffset())
	}
}

// TestOrphanTempFilesCleanedBeforeScan covers the orphan cleanup a crash
// during a compaction copy-and-swap (or an index rebuild) can leave behind:
// any ".tmp-*" file in the partition directory is deleted before scanning, and
// never confused for a real segment or index file.
func TestOrphanTempFilesCleanedBeforeScan(t *testing.T) {
	dir := t.TempDir()
	buildSegment(t, dir, 0, []string{"a"})

	orphans := []string{
		".tmp-compact-1234",
		".tmp-compact-1234.index",
		".tmp-index-5678",
	}
	for _, name := range orphans {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("garbage"), 0o644); err != nil {
			t.Fatalf("write orphan %s: %v", name, err)
		}
	}

	res := mustRun(t, dir, testIndexInterval)
	if len(res.Segments) != 1 {
		t.Fatalf("Segments = %d, want 1", len(res.Segments))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Errorf("orphan temp %q survived recovery", e.Name())
		}
	}
}
