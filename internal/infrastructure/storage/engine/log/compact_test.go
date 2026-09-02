package log

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/filesystem"
)

func compactTestCfg() Config {
	return Config{MaxSegmentBytes: 1 << 20, IndexInterval: 64, SyncMode: SyncEveryWrite, TombstoneRetention: time.Hour}
}

// appendKV appends a single keyed (or keyless, for key="") record at ts.
func appendKV(t *testing.T, l *Log, ts time.Time, key string, payload []byte) offset.Offset {
	t.Helper()
	base, err := l.Append(context.Background(), &message.RecordBatch{
		FirstTimestamp: ts,
		LastTimestamp:  ts,
		Records: []message.Record{{
			Timestamp: ts,
			Message:   message.Message{Key: key, Payload: payload},
		}},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	return base
}

// appendKVBatch appends multiple keyed records in a single batch, so the
// segment holds exactly one physical batch instead of one per record.
func appendKVBatch(t *testing.T, l *Log, ts time.Time, kvs ...[2]string) offset.Offset {
	t.Helper()
	recs := make([]message.Record, len(kvs))
	for i, kv := range kvs {
		recs[i] = message.Record{Timestamp: ts, Message: message.Message{Key: kv[0], Payload: []byte(kv[1])}}
	}
	base, err := l.Append(context.Background(), &message.RecordBatch{
		FirstTimestamp: ts,
		LastTimestamp:  ts,
		Records:        recs,
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	return base
}

// seal rotates the active segment, sealing it as a compaction candidate.
func seal(t *testing.T, l *Log) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.rotate(); err != nil {
		t.Fatalf("rotate() error = %v", err)
	}
}

func recordAt(t *testing.T, recs []message.Record, o offset.Offset) message.Record {
	t.Helper()
	for _, r := range recs {
		if r.Offset == o {
			return r
		}
	}
	t.Fatalf("no record at offset %v in %+v", o, recs)
	return message.Record{}
}

func TestCompactDedupeWithinOneSegment(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	now := time.Unix(1700000000, 0).UTC()
	appendKV(t, l, now, "a", []byte("v1"))
	appendKV(t, l, now, "a", []byte("v2"))
	appendKV(t, l, now, "a", []byte("v3"))
	seal(t, l)

	res, err := l.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if res.Segments != 1 {
		t.Fatalf("Segments = %d, want 1", res.Segments)
	}

	recs := readAll(t, l, 0)
	if len(recs) != 1 {
		t.Fatalf("records = %+v, want exactly the newest survivor", recs)
	}
	if recs[0].Offset != 2 || string(recs[0].Message.Payload) != "v3" {
		t.Errorf("survivor = %+v, want offset 2 payload v3", recs[0])
	}
}

func TestCompactNewestWinsAcrossSegments(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	now := time.Unix(1700000000, 0).UTC()
	appendKV(t, l, now, "a", []byte("v1")) // offset 0
	seal(t, l)
	appendKV(t, l, now, "a", []byte("v2")) // offset 1
	seal(t, l)

	res, err := l.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	// The first sealed segment (offset 0) is fully superseded and deleted; the
	// second (offset 1, the newest) has nothing to drop, so it is left as-is
	// by the gain gate. Either way one segment is "touched" this run.
	if res.Segments != 1 {
		t.Fatalf("Segments = %d, want 1 (the fully-superseded segment deleted)", res.Segments)
	}

	recs := readAll(t, l, 0)
	if len(recs) != 1 || recs[0].Offset != 1 || string(recs[0].Message.Payload) != "v2" {
		t.Fatalf("records = %+v, want exactly offset 1 payload v2", recs)
	}
}

func TestCompactKeylessPreserved(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	now := time.Unix(1700000000, 0).UTC()
	appendKV(t, l, now, "", []byte("keyless-1")) // offset 0, always kept
	appendKV(t, l, now, "a", []byte("v1"))       // offset 1, superseded
	appendKV(t, l, now, "", []byte("keyless-2")) // offset 2, always kept
	appendKV(t, l, now, "a", []byte("v2"))       // offset 3, newest for "a"
	seal(t, l)

	if _, err := l.Compact(context.Background()); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	recs := readAll(t, l, 0)
	offsets := make(map[offset.Offset]string, len(recs))
	for _, r := range recs {
		offsets[r.Offset] = string(r.Message.Payload)
	}
	if offsets[0] != "keyless-1" || offsets[2] != "keyless-2" {
		t.Errorf("keyless records not preserved verbatim: %+v", offsets)
	}
	if offsets[3] != "v2" {
		t.Errorf("newest keyed survivor missing: %+v", offsets)
	}
	if _, ok := offsets[1]; ok {
		t.Errorf("superseded keyed record at offset 1 should have been dropped: %+v", offsets)
	}
}

func TestCompactTombstoneRetainedWithinWindow(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	t0 := time.Unix(1700000000, 0).UTC()
	appendKV(t, l, t0, "t", []byte("v1"))    // offset 0, superseded
	appendKV(t, l, t0, "t", nil)             // offset 1, tombstone
	appendKV(t, l, t0, "keep", []byte("ok")) // offset 2, unrelated, keeps the segment from being fully empty
	seal(t, l)

	// now is inside the retention window: the tombstone must survive and keep
	// suppressing the older value.
	now := t0.Add(30 * time.Minute)
	res, err := l.compactAt(context.Background(), now)
	if err != nil {
		t.Fatalf("compactAt() error = %v", err)
	}
	if res.TombstonesRemoved != 0 {
		t.Errorf("TombstonesRemoved = %d, want 0 (still within retention)", res.TombstonesRemoved)
	}

	recs := readAll(t, l, 0)
	offsets := make(map[offset.Offset]message.Record, len(recs))
	for _, r := range recs {
		offsets[r.Offset] = r
	}
	if _, ok := offsets[0]; ok {
		t.Errorf("superseded value at offset 0 should have been dropped: %+v", offsets)
	}
	tomb, ok := offsets[1]
	if !ok || tomb.Message.Payload != nil {
		t.Fatalf("tombstone at offset 1 not retained as a nil payload: %+v", offsets)
	}
	if offsets[2].Message.Payload == nil || string(offsets[2].Message.Payload) != "ok" {
		t.Errorf("unrelated record at offset 2 not preserved: %+v", offsets)
	}
}

func TestCompactExpiredTombstoneDropped(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	t0 := time.Unix(1700000000, 0).UTC()
	appendKV(t, l, t0, "t", []byte("v1")) // offset 0, superseded
	appendKV(t, l, t0, "t", nil)          // offset 1, tombstone, will expire
	seal(t, l)

	// now is well past TombstoneRetention (1h): the tombstone itself must be
	// dropped too, taking its already-superseded value with it. The segment
	// then has no survivors and is deleted.
	now := t0.Add(2 * time.Hour)
	res, err := l.compactAt(context.Background(), now)
	if err != nil {
		t.Fatalf("compactAt() error = %v", err)
	}
	if res.TombstonesRemoved != 1 {
		t.Errorf("TombstonesRemoved = %d, want 1", res.TombstonesRemoved)
	}
	if res.Segments != 1 {
		t.Errorf("Segments = %d, want 1 (fully-empty segment deleted)", res.Segments)
	}

	recs := readAll(t, l, 0)
	if len(recs) != 0 {
		t.Fatalf("records = %+v, want none: the key's whole history expired", recs)
	}
}

func TestCompactGainGateSkipsLowYieldSegment(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	now := time.Unix(1700000000, 0).UTC()
	// Every key is unique and both records already share one batch, so a
	// rewrite would reproduce byte-for-byte the same single batch: 0% shrink,
	// below MinCompactGain.
	appendKVBatch(t, l, now, [2]string{"a", "v1"}, [2]string{"b", "v2"})
	seal(t, l)

	before := readAll(t, l, 0)

	res, err := l.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if res.Segments != 0 {
		t.Errorf("Segments = %d, want 0: nothing should have been rewritten", res.Segments)
	}

	after := readAll(t, l, 0)
	if len(after) != len(before) {
		t.Fatalf("records changed across a no-op compaction: before %+v, after %+v", before, after)
	}
	for i := range before {
		if before[i].Offset != after[i].Offset || string(before[i].Message.Payload) != string(after[i].Message.Payload) {
			t.Errorf("record %d changed: before %+v, after %+v", i, before[i], after[i])
		}
	}
}

func TestCompactNeverRewritesActiveSegment(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	now := time.Unix(1700000000, 0).UTC()
	// A sealed segment so Compact has something to do.
	appendKV(t, l, now, "sealed-key", []byte("sealed-val"))
	seal(t, l)

	// Duplicate keys live only in the active segment, which must never be
	// rewritten: both must survive untouched.
	appendKV(t, l, now, "active", []byte("v1")) // offset 1
	appendKV(t, l, now, "active", []byte("v2")) // offset 2

	leoBefore := l.NextOffset()
	if _, err := l.Compact(context.Background()); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if l.NextOffset() != leoBefore {
		t.Errorf("NextOffset() changed from %v to %v", leoBefore, l.NextOffset())
	}

	recs := readAll(t, l, 0)
	v1 := recordAt(t, recs, 1)
	v2 := recordAt(t, recs, 2)
	if string(v1.Message.Payload) != "v1" || string(v2.Message.Payload) != "v2" {
		t.Errorf("active segment's duplicate keys were deduplicated: %+v, %+v", v1, v2)
	}
}

func TestCompactHolesVisibleToRead(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	now := time.Unix(1700000000, 0).UTC()
	appendKV(t, l, now, "a", []byte("old")) // offset 0, dropped
	appendKV(t, l, now, "a", []byte("new")) // offset 1, survivor
	seal(t, l)

	if _, err := l.Compact(context.Background()); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	// A read starting at the removed offset must transparently return the
	// next surviving record rather than erroring or renumbering it.
	recs := readAll(t, l, 0)
	if len(recs) != 1 || recs[0].Offset != 1 || string(recs[0].Message.Payload) != "new" {
		t.Fatalf("Read(0) = %+v, want exactly offset 1 payload new", recs)
	}
}

func TestCompactConcurrentCallsSerialize(t *testing.T) {
	_, l := newTestLog(t, compactTestCfg())
	now := time.Unix(1700000000, 0).UTC()
	appendKV(t, l, now, "a", []byte("v1"))
	appendKV(t, l, now, "a", []byte("v2"))
	seal(t, l)

	l.mu.Lock()
	l.compacting = true
	l.mu.Unlock()

	res, err := l.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if res.Segments != 0 {
		t.Errorf("Segments = %d, want 0: a concurrent compaction was already in progress", res.Segments)
	}

	l.mu.Lock()
	l.compacting = false
	l.mu.Unlock()
}

// TestOpenLogAfterCompactionRecoversCorrectly closes a log right after a
// compaction and reopens it, proving the compacted, sparse-offset segment
// round-trips through the ordinary snapshot recovery path.
func TestOpenLogAfterCompactionRecoversCorrectly(t *testing.T) {
	root, l := newTestLog(t, compactTestCfg())
	name, _ := topic.NewName("orders")
	now := time.Unix(1700000000, 0).UTC()

	appendKV(t, l, now, "a", []byte("old")) // offset 0, dropped
	appendKV(t, l, now, "a", []byte("new")) // offset 1, survivor
	appendKV(t, l, now, "", []byte("kl"))   // offset 2, keyless, always kept
	seal(t, l)
	appendKV(t, l, now, "tail", []byte("t")) // offset 3, stays in the active segment

	if _, err := l.Compact(context.Background()); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	before := readAll(t, l, 0)

	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenLog(root, name, partition.ID(0), compactTestCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()

	after := readAll(t, reopened, 0)
	if len(after) != len(before) {
		t.Fatalf("recovered %d records, want %d: %+v vs %+v", len(after), len(before), after, before)
	}
	for i := range before {
		if before[i].Offset != after[i].Offset || string(before[i].Message.Payload) != string(after[i].Message.Payload) {
			t.Errorf("record %d = %+v, want %+v", i, after[i], before[i])
		}
	}
	if reopened.NextOffset() != l.NextOffset() {
		t.Errorf("NextOffset() = %v, want %v", reopened.NextOffset(), l.NextOffset())
	}
}

// TestRecoveryHealsCrashBetweenDataAndIndexRename simulates the copy-and-swap
// window docs/compaction-design.md sec 6 calls out: the compacted data file
// was renamed into place but the index rename did not happen before the
// crash, so the old (pre-compaction) index is left beside the new data. The
// hardened restoreFromIndex per-entry check must detect the mismatch and fall
// back to a full scan rather than trusting the stale index.
func TestRecoveryHealsCrashBetweenDataAndIndexRename(t *testing.T) {
	root, l := newTestLog(t, compactTestCfg())
	name, _ := topic.NewName("orders")
	now := time.Unix(1700000000, 0).UTC()

	appendKV(t, l, now, "a", []byte("old")) // offset 0, dropped
	appendKV(t, l, now, "a", []byte("new")) // offset 1, survivor
	seal(t, l)
	dir := l.dir

	// Save the pre-compaction index bytes before compacting.
	staleIndex, err := os.ReadFile(filesystem.SegmentIndexPath(dir, 0))
	if err != nil {
		t.Fatalf("read pre-compaction index: %v", err)
	}

	if _, err := l.Compact(context.Background()); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	before := readAll(t, l, 0)
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Simulate the crash window: put the old index back over the compacted
	// segment's (already-renamed) new index file.
	if err := os.WriteFile(filesystem.SegmentIndexPath(dir, 0), staleIndex, 0o644); err != nil {
		t.Fatalf("plant stale index: %v", err)
	}
	// The snapshot still describes the post-compaction state, so recovery
	// takes the fast path and must catch the mismatch itself.

	reopened, err := OpenLog(root, name, partition.ID(0), compactTestCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()

	after := readAll(t, reopened, 0)
	if len(after) != len(before) || after[0].Offset != before[0].Offset ||
		string(after[0].Message.Payload) != string(before[0].Message.Payload) {
		t.Fatalf("recovery trusted the stale index: got %+v, want %+v", after, before)
	}
}
