package recovery

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/checksum"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/index"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/segment"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/snapshot"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/filesystem"
)

// testIndexInterval puts one index entry on every batch, so a rebuilt index is
// observable batch by batch.
const testIndexInterval = 1

// builtSegment records where every batch of a hand-built segment file landed,
// so tests can corrupt or checkpoint an exact batch boundary.
type builtSegment struct {
	path   string
	base   offset.Offset
	starts []int64
	bases  []offset.Offset
	size   int64
	next   offset.Offset
}

// buildSegment writes the segment file for base in dir, one batch per group of
// payloads.
func buildSegment(t *testing.T, dir string, base offset.Offset, batches ...[]string) builtSegment {
	t.Helper()
	b := builtSegment{path: filesystem.SegmentLogPath(dir, base), base: base, next: base}
	var buf []byte
	for _, payloads := range batches {
		b.starts = append(b.starts, int64(len(buf)))
		b.bases = append(b.bases, b.next)
		buf = append(buf, encodeBatch(t, b.next, payloads...)...)
		b.next += offset.Offset(len(payloads))
	}
	if err := os.WriteFile(b.path, buf, 0o644); err != nil {
		t.Fatalf("write segment %s: %v", b.path, err)
	}
	b.size = int64(len(buf))
	return b
}

func encodeBatch(t *testing.T, base offset.Offset, payloads ...string) []byte {
	t.Helper()
	ts := time.Unix(1700000000, 0).UTC()
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
	return data
}

// zeroRecordBatch builds a CRC-valid header declaring no records, the frame a
// crash can leave behind when the header lands but the records never do.
func zeroRecordBatch(base offset.Offset) []byte {
	buf := make([]byte, codec.HeaderSize)
	buf[0] = codec.Magic
	buf[1] = codec.Version
	binary.BigEndian.PutUint64(buf[8:16], uint64(base))
	binary.BigEndian.PutUint32(buf[4:8], checksum.Sum(buf[8:]))
	return buf
}

func appendRaw(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("append to %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}

func flipByte(t *testing.T, path string, at int64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	data[at] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeIndexFile(t *testing.T, dir string, b builtSegment, upTo int) {
	t.Helper()
	ix := index.New(b.base)
	for i := 0; i < upTo; i++ {
		if err := ix.Append(uint32(b.bases[i]-b.base), uint32(b.starts[i])); err != nil {
			t.Fatalf("index.Append() error = %v", err)
		}
	}
	if err := os.WriteFile(filesystem.SegmentIndexPath(dir, b.base), ix.Encode(), 0o644); err != nil {
		t.Fatalf("write index file: %v", err)
	}
}

func writeSnapshot(t *testing.T, dir string, st snapshot.State) {
	t.Helper()
	if err := snapshot.Write(dir, st); err != nil {
		t.Fatalf("snapshot.Write() error = %v", err)
	}
}

func onDiskSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st.Size()
}

// mustRun recovers dir and closes the recovered segments when the test ends.
func mustRun(t *testing.T, dir string, indexInterval int64) *Result {
	t.Helper()
	res, err := Run(dir, indexInterval)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	t.Cleanup(func() {
		for _, s := range res.Segments {
			_ = s.Close()
		}
	})
	return res
}

// assertIndexed checks that every batch start in b is resolvable through the
// segment's sparse index, up to the first upTo batches.
func assertIndexed(t *testing.T, seg *segment.Segment, b builtSegment, upTo int) {
	t.Helper()
	for i := 0; i < upTo; i++ {
		pos, ok := seg.PositionFor(b.bases[i])
		if !ok || pos != b.starts[i] {
			t.Errorf("PositionFor(%d) = (%d, %v), want (%d, true)", b.bases[i], pos, ok, b.starts[i])
		}
	}
}

func TestRunEmptyDirectoryYieldsNoSegments(t *testing.T) {
	res := mustRun(t, t.TempDir(), testIndexInterval)
	if len(res.Segments) != 0 {
		t.Fatalf("Segments = %d, want 0", len(res.Segments))
	}
	if res.Truncated || res.SnapshotUsed {
		t.Fatalf("Result = %+v, want zero flags", *res)
	}
}

func TestRunMissingDirectoryFails(t *testing.T) {
	if _, err := Run(filepath.Join(t.TempDir(), "absent"), testIndexInterval); err == nil {
		t.Fatal("Run(missing dir) error = nil, want error")
	}
}

func TestRunRebuildsStateAndIndexFromData(t *testing.T) {
	dir := t.TempDir()
	b := buildSegment(t, dir, 0, []string{"a", "b"}, []string{"c"}, []string{"d", "e"})

	res := mustRun(t, dir, testIndexInterval)

	if len(res.Segments) != 1 {
		t.Fatalf("Segments = %d, want 1", len(res.Segments))
	}
	if res.Truncated || res.SnapshotUsed {
		t.Fatalf("Result = %+v, want a clean full scan", *res)
	}
	seg := res.Segments[0]
	if seg.NextOffset() != b.next {
		t.Errorf("NextOffset() = %v, want %v", seg.NextOffset(), b.next)
	}
	if seg.Size() != b.size {
		t.Errorf("Size() = %d, want %d", seg.Size(), b.size)
	}
	assertIndexed(t, seg, b, len(b.starts))
}

func TestRunAcrossSealedAndActiveSegments(t *testing.T) {
	dir := t.TempDir()
	s0 := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	s1 := buildSegment(t, dir, s0.next, []string{"c"}, []string{"d"})

	res := mustRun(t, dir, testIndexInterval)

	if len(res.Segments) != 2 {
		t.Fatalf("Segments = %d, want 2", len(res.Segments))
	}
	if res.Segments[0].Base() != s0.base || res.Segments[1].Base() != s1.base {
		t.Fatalf("bases = %v, %v; want %v, %v",
			res.Segments[0].Base(), res.Segments[1].Base(), s0.base, s1.base)
	}
	if res.Segments[1].NextOffset() != s1.next {
		t.Errorf("active NextOffset() = %v, want %v", res.Segments[1].NextOffset(), s1.next)
	}
}

func TestRunZeroLengthSegmentFile(t *testing.T) {
	dir := t.TempDir()
	path := filesystem.SegmentLogPath(dir, 0)
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("write empty segment: %v", err)
	}

	res := mustRun(t, dir, testIndexInterval)

	if len(res.Segments) != 1 {
		t.Fatalf("Segments = %d, want 1", len(res.Segments))
	}
	if res.Truncated {
		t.Errorf("Truncated = true, want false for an empty segment")
	}
	if res.Segments[0].Size() != 0 || res.Segments[0].NextOffset() != 0 {
		t.Errorf("Size()/NextOffset() = %d/%v, want 0/0",
			res.Segments[0].Size(), res.Segments[0].NextOffset())
	}
}

func TestTruncatesGarbageTail(t *testing.T) {
	dir := t.TempDir()
	b := buildSegment(t, dir, 0, []string{"a", "b"}, []string{"c"})
	appendRaw(t, b.path, []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04})

	res := mustRun(t, dir, testIndexInterval)

	if !res.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	if res.TruncatedBytes != 8 {
		t.Errorf("TruncatedBytes = %d, want 8", res.TruncatedBytes)
	}
	if res.Segments[0].NextOffset() != b.next {
		t.Errorf("NextOffset() = %v, want %v", res.Segments[0].NextOffset(), b.next)
	}
	if got := onDiskSize(t, b.path); got != b.size {
		t.Errorf("on-disk size = %d, want %d (tail not removed)", got, b.size)
	}
}

func TestTruncatesPartialTrailingBatch(t *testing.T) {
	dir := t.TempDir()
	b := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	// A batch whose header landed but whose records section did not.
	partial := encodeBatch(t, b.next, "c")[:codec.HeaderSize+4]
	appendRaw(t, b.path, partial)

	res := mustRun(t, dir, testIndexInterval)

	if !res.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	if res.TruncatedBytes != int64(len(partial)) {
		t.Errorf("TruncatedBytes = %d, want %d", res.TruncatedBytes, len(partial))
	}
	if res.Segments[0].NextOffset() != b.next {
		t.Errorf("NextOffset() = %v, want %v", res.Segments[0].NextOffset(), b.next)
	}
	if got := onDiskSize(t, b.path); got != b.size {
		t.Errorf("on-disk size = %d, want %d", got, b.size)
	}
}

func TestTruncatesZeroRecordTrailingBatch(t *testing.T) {
	dir := t.TempDir()
	b := buildSegment(t, dir, 0, []string{"a"})
	appendRaw(t, b.path, zeroRecordBatch(b.next))

	res := mustRun(t, dir, testIndexInterval)

	if !res.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	if res.TruncatedBytes != codec.HeaderSize {
		t.Errorf("TruncatedBytes = %d, want %d", res.TruncatedBytes, codec.HeaderSize)
	}
	if res.Segments[0].NextOffset() != b.next {
		t.Errorf("NextOffset() = %v, want %v", res.Segments[0].NextOffset(), b.next)
	}
}

func TestTruncatesCorruptCrcTailBatch(t *testing.T) {
	dir := t.TempDir()
	b := buildSegment(t, dir, 0, []string{"a", "b"}, []string{"c"}, []string{"d"})
	flipByte(t, b.path, b.starts[2]+codec.HeaderSize) // first payload byte of the last batch

	res := mustRun(t, dir, testIndexInterval)

	if !res.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	if want := b.size - b.starts[2]; res.TruncatedBytes != want {
		t.Errorf("TruncatedBytes = %d, want %d", res.TruncatedBytes, want)
	}
	if res.Segments[0].NextOffset() != b.bases[2] {
		t.Errorf("NextOffset() = %v, want %v", res.Segments[0].NextOffset(), b.bases[2])
	}
	if got := onDiskSize(t, b.path); got != b.starts[2] {
		t.Errorf("on-disk size = %d, want %d", got, b.starts[2])
	}
}

func TestTruncatesOffsetDiscontinuity(t *testing.T) {
	dir := t.TempDir()
	b := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	// A CRC-valid batch claiming an offset the log never reached.
	appendRaw(t, b.path, encodeBatch(t, b.next+7, "c"))

	res := mustRun(t, dir, testIndexInterval)

	if !res.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	if res.Segments[0].NextOffset() != b.next {
		t.Errorf("NextOffset() = %v, want %v", res.Segments[0].NextOffset(), b.next)
	}
}

func TestTruncatedSegmentKeepsRebuiltIndex(t *testing.T) {
	dir := t.TempDir()
	b := buildSegment(t, dir, 0, []string{"a"}, []string{"b"}, []string{"c"})
	appendRaw(t, b.path, []byte{0xDE, 0xAD, 0xBE, 0xEF})

	res := mustRun(t, dir, testIndexInterval)

	if !res.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	// The surviving batches were scanned, so their index entries must survive
	// the truncation too: recovery promises a rebuilt sparse index.
	assertIndexed(t, res.Segments[0], b, len(b.starts))
}

func TestCorruptSealedSegmentIsFatal(t *testing.T) {
	dir := t.TempDir()
	sealed := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	buildSegment(t, dir, sealed.next, []string{"c"})
	flipByte(t, sealed.path, sealed.starts[1]+codec.HeaderSize)

	if _, err := Run(dir, testIndexInterval); !errors.Is(err, codec.ErrCrcMismatch) {
		t.Fatalf("Run() error = %v, want ErrCrcMismatch", err)
	}
}

func TestFatalRecoveryClosesAlreadyOpenedSegments(t *testing.T) {
	dir := t.TempDir()
	s0 := buildSegment(t, dir, 0, []string{"a"})
	s1 := buildSegment(t, dir, s0.next, []string{"b"}, []string{"c"})
	buildSegment(t, dir, s1.next, []string{"d"})
	flipByte(t, s1.path, s1.starts[1]+codec.HeaderSize)

	if _, err := Run(dir, testIndexInterval); !errors.Is(err, codec.ErrCrcMismatch) {
		t.Fatalf("Run() error = %v, want ErrCrcMismatch", err)
	}
	// The first segment was already open when recovery aborted. A leaked handle
	// costs a descriptor and, on Windows, locks the partition directory.
	if err := os.Remove(s0.path); err != nil {
		t.Fatalf("remove %s after failed recovery: %v", s0.path, err)
	}
}

func TestSnapshotFastPathRestoresFromIndexFiles(t *testing.T) {
	dir := t.TempDir()
	s0 := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	s1 := buildSegment(t, dir, s0.next, []string{"c"}, []string{"d"})
	writeIndexFile(t, dir, s0, len(s0.starts))
	writeIndexFile(t, dir, s1, len(s1.starts))
	writeSnapshot(t, dir, snapshot.State{
		LEO: s1.next, ActiveBase: s1.base, ActiveSize: s1.size, ActiveNext: s1.next,
	})

	res := mustRun(t, dir, testIndexInterval)

	if !res.SnapshotUsed {
		t.Fatal("SnapshotUsed = false, want true")
	}
	if res.Truncated {
		t.Errorf("Truncated = true, want false")
	}
	if len(res.Segments) != 2 {
		t.Fatalf("Segments = %d, want 2", len(res.Segments))
	}
	if res.Segments[1].NextOffset() != s1.next {
		t.Errorf("active NextOffset() = %v, want %v", res.Segments[1].NextOffset(), s1.next)
	}
	assertIndexed(t, res.Segments[0], s0, len(s0.starts))
	assertIndexed(t, res.Segments[1], s1, len(s1.starts))
}

func TestSnapshotNamingAnotherActiveSegmentIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s0 := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	s1 := buildSegment(t, dir, s0.next, []string{"c"})
	// The checkpoint still names the first segment as active: the log rotated
	// after it was written.
	writeSnapshot(t, dir, snapshot.State{
		LEO: s0.next, ActiveBase: s0.base, ActiveSize: s0.size, ActiveNext: s0.next,
	})

	res := mustRun(t, dir, testIndexInterval)

	if res.SnapshotUsed {
		t.Fatal("SnapshotUsed = true, want a full scan")
	}
	if len(res.Segments) != 2 || res.Segments[1].NextOffset() != s1.next {
		t.Fatalf("Segments = %d, active NextOffset = %v; want 2 and %v",
			len(res.Segments), res.Segments[len(res.Segments)-1].NextOffset(), s1.next)
	}
	if _, err := os.Stat(snapshot.Path(dir)); !os.IsNotExist(err) {
		t.Errorf("stale snapshot not removed (stat err = %v)", err)
	}
}

func TestSnapshotAheadOfDataIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	// A checkpoint claiming more durable bytes than the file holds: the tail it
	// describes never reached the disk.
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.next + 5, ActiveBase: s.base, ActiveSize: s.size + 100, ActiveNext: s.next + 5,
	})

	res := mustRun(t, dir, testIndexInterval)

	if res.SnapshotUsed {
		t.Fatal("SnapshotUsed = true, want a full scan")
	}
	if res.Segments[0].NextOffset() != s.next {
		t.Errorf("NextOffset() = %v, want %v", res.Segments[0].NextOffset(), s.next)
	}
	if _, err := os.Stat(snapshot.Path(dir)); !os.IsNotExist(err) {
		t.Errorf("stale snapshot not removed (stat err = %v)", err)
	}
}

func TestCorruptSnapshotIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.next, ActiveBase: s.base, ActiveSize: s.size, ActiveNext: s.next,
	})
	flipByte(t, snapshot.Path(dir), 30)

	res := mustRun(t, dir, testIndexInterval)

	if res.SnapshotUsed {
		t.Fatal("SnapshotUsed = true, want a full scan")
	}
	if res.Segments[0].NextOffset() != s.next {
		t.Errorf("NextOffset() = %v, want %v", res.Segments[0].NextOffset(), s.next)
	}
	if _, err := os.Stat(snapshot.Path(dir)); !os.IsNotExist(err) {
		t.Errorf("corrupt snapshot not removed (stat err = %v)", err)
	}
}

func TestSnapshotDeltaTailTornIsTruncated(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"}, []string{"c"})
	writeIndexFile(t, dir, s, 2)
	// The checkpoint covers the first two batches; the third was written after
	// it and its write was torn.
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.bases[2], ActiveBase: s.base, ActiveSize: s.starts[2], ActiveNext: s.bases[2],
	})
	flipByte(t, s.path, s.starts[2]+codec.HeaderSize)

	res := mustRun(t, dir, testIndexInterval)

	if !res.SnapshotUsed {
		t.Fatal("SnapshotUsed = false, want true")
	}
	if !res.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	if want := s.size - s.starts[2]; res.TruncatedBytes != want {
		t.Errorf("TruncatedBytes = %d, want %d", res.TruncatedBytes, want)
	}
	if res.Segments[0].NextOffset() != s.bases[2] {
		t.Errorf("NextOffset() = %v, want %v", res.Segments[0].NextOffset(), s.bases[2])
	}
	if got := onDiskSize(t, s.path); got != s.starts[2] {
		t.Errorf("on-disk size = %d, want %d", got, s.starts[2])
	}
	// The checkpointed prefix was durable; its index entries must survive.
	assertIndexed(t, res.Segments[0], s, 2)
}

func TestSnapshotDeltaTailIntactIsKept(t *testing.T) {
	dir := t.TempDir()
	s := buildSegment(t, dir, 0, []string{"a"}, []string{"b"}, []string{"c"})
	writeIndexFile(t, dir, s, 2)
	writeSnapshot(t, dir, snapshot.State{
		LEO: s.bases[2], ActiveBase: s.base, ActiveSize: s.starts[2], ActiveNext: s.bases[2],
	})

	res := mustRun(t, dir, testIndexInterval)

	if !res.SnapshotUsed || res.Truncated {
		t.Fatalf("Result = %+v, want SnapshotUsed without truncation", *res)
	}
	if res.Segments[0].NextOffset() != s.next {
		t.Errorf("NextOffset() = %v, want %v (delta tail kept)", res.Segments[0].NextOffset(), s.next)
	}
	assertIndexed(t, res.Segments[0], s, len(s.starts))
}

func TestSnapshotSealedIndexCorruptFallsBackToScan(t *testing.T) {
	dir := t.TempDir()
	s0 := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	s1 := buildSegment(t, dir, s0.next, []string{"c"})
	// A trailing partial entry makes the sealed segment's index undecodable.
	if err := os.WriteFile(filesystem.SegmentIndexPath(dir, s0.base), []byte{0x00, 0x01, 0x02}, 0o644); err != nil {
		t.Fatalf("write corrupt index: %v", err)
	}
	writeIndexFile(t, dir, s1, len(s1.starts))
	writeSnapshot(t, dir, snapshot.State{
		LEO: s1.next, ActiveBase: s1.base, ActiveSize: s1.size, ActiveNext: s1.next,
	})

	res := mustRun(t, dir, testIndexInterval)

	if !res.SnapshotUsed {
		t.Fatal("SnapshotUsed = false, want true")
	}
	if len(res.Segments) != 2 {
		t.Fatalf("Segments = %d, want 2", len(res.Segments))
	}
	// The sealed segment was rescanned from its data, so its index is back.
	assertIndexed(t, res.Segments[0], s0, len(s0.starts))
}

func TestSnapshotSealedSegmentCorruptIsFatal(t *testing.T) {
	dir := t.TempDir()
	s0 := buildSegment(t, dir, 0, []string{"a"}, []string{"b"})
	s1 := buildSegment(t, dir, s0.next, []string{"c"})
	// No index file for the sealed segment forces a scan, which validates it.
	writeIndexFile(t, dir, s1, len(s1.starts))
	writeSnapshot(t, dir, snapshot.State{
		LEO: s1.next, ActiveBase: s1.base, ActiveSize: s1.size, ActiveNext: s1.next,
	})
	flipByte(t, s0.path, s0.starts[1]+codec.HeaderSize)

	if _, err := Run(dir, testIndexInterval); !errors.Is(err, codec.ErrCrcMismatch) {
		t.Fatalf("Run() error = %v, want ErrCrcMismatch", err)
	}
}
