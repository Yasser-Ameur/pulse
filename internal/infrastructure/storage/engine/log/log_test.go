package log

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/retention"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/snapshot"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/filesystem"
)

func testCfg() Config {
	return Config{MaxSegmentBytes: 1 << 20, IndexInterval: 64, SyncMode: SyncEveryWrite}
}

func newTestLog(t *testing.T, cfg Config) (string, *Log) {
	t.Helper()
	root := t.TempDir()
	name, _ := topic.NewName("orders")
	l, err := CreateLog(root, name, partition.ID(0), cfg, nil)
	if err != nil {
		t.Fatalf("CreateLog() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return root, l
}

func appendBatch(t *testing.T, l *Log, payloads ...string) offset.Offset {
	t.Helper()
	now := time.Unix(1700000000, 0).UTC()
	recs := make([]message.Record, 0, len(payloads))
	for _, p := range payloads {
		recs = append(recs, message.Record{
			Timestamp: now,
			Message:   message.Message{Payload: []byte(p)},
		})
	}
	base, err := l.Append(context.Background(), &message.RecordBatch{
		FirstTimestamp: now,
		LastTimestamp:  now,
		Records:        recs,
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	return base
}

func readAll(t *testing.T, l *Log, from offset.Offset) []message.Record {
	t.Helper()
	recs, err := l.Read(context.Background(), from, 0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	return recs
}

func payloads(recs []message.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Message.Payload)
	}
	return out
}

func TestAppendAssignsOffsets(t *testing.T) {
	_, l := newTestLog(t, testCfg())
	now := time.Unix(1700000000, 0).UTC()
	batch := &message.RecordBatch{
		FirstTimestamp: now,
		LastTimestamp:  now,
		Records: []message.Record{
			{Message: message.Message{Payload: []byte("a")}},
			{Message: message.Message{Payload: []byte("b")}},
			{Message: message.Message{Payload: []byte("c")}},
		},
	}
	base, err := l.Append(context.Background(), batch)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if base != 0 || batch.BaseOffset != 0 {
		t.Fatalf("base = %v, batch.BaseOffset = %v; want 0", base, batch.BaseOffset)
	}
	for i, r := range batch.Records {
		if r.Offset != offset.Offset(i) {
			t.Errorf("record %d offset = %v, want %d", i, r.Offset, i)
		}
	}
	if l.NextOffset() != offset.Offset(3) {
		t.Fatalf("NextOffset() = %v, want 3", l.NextOffset())
	}

	if base2 := appendBatch(t, l, "d"); base2 != 3 {
		t.Fatalf("second base = %v, want 3", base2)
	}
	if l.NextOffset() != offset.Offset(4) {
		t.Fatalf("NextOffset() = %v, want 4", l.NextOffset())
	}
}

func TestReadSequentialAndBounds(t *testing.T) {
	_, l := newTestLog(t, testCfg())
	appendBatch(t, l, "a", "b", "c", "d", "e")

	all := readAll(t, l, 0)
	if len(all) != 5 {
		t.Fatalf("read %d records, want 5", len(all))
	}
	if got := payloads(all); got[0] != "a" || got[4] != "e" {
		t.Fatalf("payloads = %v", got)
	}

	sub := readAll(t, l, 3)
	if len(sub) != 2 || string(sub[0].Message.Payload) != "d" {
		t.Fatalf("read from 3 = %v, want [d e]", payloads(sub))
	}

	limited, err := l.Read(context.Background(), 0, 2, 0)
	if err != nil || len(limited) != 2 {
		t.Fatalf("Read(limit=2) = %d records, %v; want 2", len(limited), err)
	}

	beyond := readAll(t, l, 100)
	if len(beyond) != 0 {
		t.Fatalf("Read(100) = %v, want empty", payloads(beyond))
	}
	if invalid, err := l.Read(context.Background(), offset.Invalid, 0, 0); err == nil {
		t.Fatalf("Read(invalid) = %v, nil; want error", invalid)
	}
}

func TestReadMaxBytes(t *testing.T) {
	_, l := newTestLog(t, testCfg())
	appendBatch(t, l, "0123456789", "abcdefghij", "klmnopqrst")

	recs, err := l.Read(context.Background(), 0, 0, 12)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("Read(maxBytes=12) = %d records, want 1", len(recs))
	}

	// A single record larger than the bound is still returned.
	appendBatch(t, l, "012345678901234567890123456789")
	recs, err = l.Read(context.Background(), 3, 0, 5)
	if err != nil || len(recs) != 1 {
		t.Fatalf("Read(single large) = %d records, %v; want 1", len(recs), err)
	}
}

func TestRotation(t *testing.T) {
	cfg := testCfg()
	cfg.MaxSegmentBytes = 200 // force rotation quickly
	_, l := newTestLog(t, cfg)

	var last offset.Offset
	for i := 0; i < 50; i++ {
		last = appendBatch(t, l, fmt.Sprintf("payload-%03d", i))
	}
	if last != 49 {
		t.Fatalf("last base = %v, want 49", last)
	}
	if l.NextOffset() != 50 {
		t.Fatalf("NextOffset() = %v, want 50", l.NextOffset())
	}

	all := readAll(t, l, 0)
	if len(all) != 50 {
		t.Fatalf("read %d records across segments, want 50", len(all))
	}
	mid := readAll(t, l, 40)
	if len(mid) != 10 || mid[0].Offset != 40 {
		t.Fatalf("read from 40 = %d records, want 10 starting at 40", len(mid))
	}
}

func TestReopenDurability(t *testing.T) {
	root, l := newTestLog(t, testCfg())
	appendBatch(t, l, "a", "b", "c")
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	name, _ := topic.NewName("orders")
	reopened, err := OpenLog(root, name, partition.ID(0), testCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if reopened.NextOffset() != 3 {
		t.Fatalf("reopened NextOffset() = %v, want 3", reopened.NextOffset())
	}
	recs := readAll(t, reopened, 0)
	if got := payloads(recs); got[0] != "a" || got[2] != "c" {
		t.Fatalf("reopened payloads = %v", got)
	}
}

func TestTornTailRecovered(t *testing.T) {
	root, l := newTestLog(t, testCfg())
	appendBatch(t, l, "a", "b", "c")
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Simulate a crash mid-write: garbage appended past the last valid batch.
	dir := filesystem.PartitionDir(root, mustTopicName(t, "orders"), partition.ID(0))
	active := lastLogFile(t, dir)
	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open active segment: %v", err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04}); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	_ = f.Close()

	name, _ := topic.NewName("orders")
	reopened, err := OpenLog(root, name, partition.ID(0), testCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if reopened.NextOffset() != 3 {
		t.Fatalf("recovered NextOffset() = %v, want 3 (torn tail truncated)", reopened.NextOffset())
	}
	if recs := readAll(t, reopened, 0); len(recs) != 3 {
		t.Fatalf("recovered records = %d, want 3", len(recs))
	}
}

func TestTornTailPartialBatch(t *testing.T) {
	root, l := newTestLog(t, testCfg())
	appendBatch(t, l, "a", "b")
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// A partial batch header (52 bytes with a bogus large batchLen).
	dir := filesystem.PartitionDir(root, mustTopicName(t, "orders"), partition.ID(0))
	active := lastLogFile(t, dir)
	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open active segment: %v", err)
	}
	header := make([]byte, 52)
	header[0] = 1
	header[1] = 1
	header[18] = 0xFF // batchLen high byte -> overruns file
	_, _ = f.Write(header)
	_ = f.Close()

	name, _ := topic.NewName("orders")
	reopened, err := OpenLog(root, name, partition.ID(0), testCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.NextOffset() != 2 {
		t.Fatalf("recovered NextOffset() = %v, want 2", reopened.NextOffset())
	}
}

func TestCorruptSealedSegmentFatal(t *testing.T) {
	cfg := testCfg()
	cfg.MaxSegmentBytes = 200
	root, l := newTestLog(t, cfg)
	for i := 0; i < 30; i++ {
		appendBatch(t, l, fmt.Sprintf("payload-%03d", i))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Flip a byte inside the first (sealed) segment's payload.
	dir := filesystem.PartitionDir(root, mustTopicName(t, "orders"), partition.ID(0))
	first := firstLogFile(t, dir)
	data, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read sealed segment: %v", err)
	}
	data[len(data)-10] ^= 0xFF
	if err := os.WriteFile(first, data, 0o644); err != nil {
		t.Fatalf("corrupt sealed segment: %v", err)
	}

	// Drop the snapshot so recovery falls back to the full scan, which
	// validates sealed segments from the data and fails loudly.
	if err := os.Remove(snapshot.Path(dir)); err != nil {
		t.Fatalf("remove snapshot: %v", err)
	}

	name, _ := topic.NewName("orders")
	if _, err := OpenLog(root, name, partition.ID(0), testCfg(), nil); err == nil {
		t.Fatal("OpenLog() error = nil, want fatal corruption error")
	}
}

func TestSnapshotSealedCorruptionDetectedOnRead(t *testing.T) {
	cfg := testCfg()
	cfg.MaxSegmentBytes = 200
	root, l := newTestLog(t, cfg)
	for i := 0; i < 30; i++ {
		appendBatch(t, l, fmt.Sprintf("payload-%03d", i))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Corrupt a sealed segment after the snapshot exists. The snapshot fast
	// path skips the data scan, so recovery succeeds; the corruption surfaces
	// as a read-time decode error instead of a startup failure.
	dir := filesystem.PartitionDir(root, mustTopicName(t, "orders"), partition.ID(0))
	first := firstLogFile(t, dir)
	data, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read sealed segment: %v", err)
	}
	data[len(data)-10] ^= 0xFF
	if err := os.WriteFile(first, data, 0o644); err != nil {
		t.Fatalf("corrupt sealed segment: %v", err)
	}

	name, _ := topic.NewName("orders")
	reopened, err := OpenLog(root, name, partition.ID(0), testCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v, want success (snapshot fast path)", err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.Read(context.Background(), 0, 0, 0); err == nil {
		t.Fatal("Read() error = nil, want crc error from corrupted sealed segment")
	}
}

func TestSnapshotRecoveryAfterCrashTornTail(t *testing.T) {
	cfg := testCfg()
	cfg.MaxSegmentBytes = 200
	root, l := newTestLog(t, cfg)
	for i := 0; i < 20; i++ {
		appendBatch(t, l, fmt.Sprintf("payload-%03d", i))
	}
	// Close writes a snapshot, then more appends grow the active segment past
	// the checkpoint (the crash window between snapshot and next close).
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	dir := filesystem.PartitionDir(root, mustTopicName(t, "orders"), partition.ID(0))

	// Simulate a crash mid-write on the active segment after the snapshot.
	active := lastLogFile(t, dir)
	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open active segment: %v", err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04}); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	_ = f.Close()

	name, _ := topic.NewName("orders")
	reopened, err := OpenLog(root, name, partition.ID(0), testCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.NextOffset() != 20 {
		t.Fatalf("recovered NextOffset() = %v, want 20 (snapshot prefix + torn tail truncated)", reopened.NextOffset())
	}
	if recs := readAll(t, reopened, 0); len(recs) != 20 {
		t.Fatalf("recovered records = %d, want 20", len(recs))
	}
}

func TestCorruptSnapshotFallsBackToFullScan(t *testing.T) {
	root, l := newTestLog(t, testCfg())
	appendBatch(t, l, "a", "b", "c")
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dir := filesystem.PartitionDir(root, mustTopicName(t, "orders"), partition.ID(0))
	snapPath := snapshot.Path(dir)
	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	data[30] ^= 0xFF // corrupt the payload; CRC no longer matches
	if err := os.WriteFile(snapPath, data, 0o644); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}

	name, _ := topic.NewName("orders")
	reopened, err := OpenLog(root, name, partition.ID(0), testCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v, want full-scan fallback", err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.NextOffset() != 3 {
		t.Fatalf("recovered NextOffset() = %v, want 3", reopened.NextOffset())
	}
	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Fatalf("corrupt snapshot was not removed (stat err = %v)", err)
	}
}

func TestTruncate(t *testing.T) {
	root, l := newTestLog(t, testCfg())
	appendBatch(t, l, "a", "b")
	appendBatch(t, l, "c", "d")
	appendBatch(t, l, "e") // offsets 0..4, LEO 5

	// Truncate is batch-atomic: removing offset 2 and up drops the whole
	// second batch, keeping [a b].
	if err := l.Truncate(2); err != nil {
		t.Fatalf("Truncate(2) error = %v", err)
	}
	if l.NextOffset() != 2 {
		t.Fatalf("NextOffset() = %v, want 2", l.NextOffset())
	}
	if got := payloads(readAll(t, l, 0)); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("after truncate = %v, want [a b]", got)
	}

	// New appends resume at the truncated LEO.
	if base := appendBatch(t, l, "f"); base != 2 {
		t.Fatalf("append after truncate base = %v, want 2", base)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	name, _ := topic.NewName("orders")
	reopened, err := OpenLog(root, name, partition.ID(0), testCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.NextOffset() != 3 {
		t.Fatalf("reopened NextOffset() = %v, want 3", reopened.NextOffset())
	}
}

func TestAppendAfterCloseFails(t *testing.T) {
	_, l := newTestLog(t, testCfg())
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := l.Append(context.Background(), &message.RecordBatch{
		Records: []message.Record{{Message: message.Message{Payload: []byte("x")}}},
	}); err != ErrClosed {
		t.Fatalf("Append() error = %v, want ErrClosed", err)
	}
}

func TestNotifyOnAppend(t *testing.T) {
	_, l := newTestLog(t, testCfg())
	ch1 := l.Notify()
	if ch1 == nil {
		t.Fatal("Notify() = nil")
	}
	appendBatch(t, l, "x")
	select {
	case <-ch1:
	case <-time.After(time.Second):
		t.Fatal("Notify() channel not closed after append")
	}
}

func appendAt(t *testing.T, l *Log, ts time.Time, payloads ...string) offset.Offset {
	t.Helper()
	recs := make([]message.Record, 0, len(payloads))
	for _, p := range payloads {
		recs = append(recs, message.Record{
			Timestamp: ts,
			Message:   message.Message{Payload: []byte(p)},
		})
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

func TestTrimZeroPolicyNoOp(t *testing.T) {
	_, l := newTestLog(t, testCfg())
	appendBatch(t, l, "a", "b", "c")
	res, err := l.Trim(time.Now(), retention.Policy{})
	if err != nil {
		t.Fatalf("Trim() error = %v", err)
	}
	if res.Segments != 0 || res.Bytes != 0 {
		t.Fatalf("Trim() = %+v, want zero result", res)
	}
	if l.NextOffset() != 3 {
		t.Fatalf("NextOffset() = %v, want 3", l.NextOffset())
	}
}

func TestTrimTimeBased(t *testing.T) {
	cfg := testCfg()
	cfg.MaxSegmentBytes = 200
	root, l := newTestLog(t, cfg)
	now := time.Now()
	old := now.Add(-48 * time.Hour)
	recent := now.Add(-10 * time.Minute)
	for i := 0; i < 30; i++ {
		appendAt(t, l, old, fmt.Sprintf("old-%03d", i))
	}
	for i := 0; i < 5; i++ {
		appendAt(t, l, recent, fmt.Sprintf("new-%03d", i))
	}
	if len(l.segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(l.segments))
	}

	res, err := l.Trim(now, retention.Policy{MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("Trim() error = %v", err)
	}
	if res.Segments == 0 {
		t.Fatal("Trim() removed 0 segments, want > 0")
	}
	if res.Bytes <= 0 {
		t.Fatalf("Trim() bytes = %d, want > 0", res.Bytes)
	}
	recs := readAll(t, l, 0)
	if len(recs) != 5 {
		t.Fatalf("records after trim = %d, want 5", len(recs))
	}
	if string(recs[0].Message.Payload) != "new-000" {
		t.Fatalf("first record after trim = %q, want new-000", recs[0].Message.Payload)
	}
	if l.NextOffset() != 35 {
		t.Fatalf("NextOffset() = %v, want 35 (offsets preserved)", l.NextOffset())
	}

	// The durable snapshot remains valid after trimming: reopening must yield
	// the same surviving view without a full scan.
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	name, _ := topic.NewName("orders")
	reopened, err := OpenLog(root, name, partition.ID(0), testCfg(), nil)
	if err != nil {
		t.Fatalf("OpenLog() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if recs := readAll(t, reopened, 0); len(recs) != 5 {
		t.Fatalf("reopened records after trim = %d, want 5", len(recs))
	}
	if reopened.NextOffset() != 35 {
		t.Fatalf("reopened NextOffset() = %v, want 35", reopened.NextOffset())
	}
}

func TestTrimSizeBased(t *testing.T) {
	cfg := testCfg()
	cfg.MaxSegmentBytes = 200
	_, l := newTestLog(t, cfg)
	now := time.Unix(1700000000, 0).UTC()
	for i := 0; i < 40; i++ {
		appendAt(t, l, now, fmt.Sprintf("payload-%03d", i))
	}
	if len(l.segments) < 3 {
		t.Fatalf("expected multiple segments, got %d", len(l.segments))
	}
	total := int64(0)
	for _, s := range l.segments {
		total += s.Size()
	}

	res, err := l.Trim(time.Now(), retention.Policy{MaxBytes: total / 2})
	if err != nil {
		t.Fatalf("Trim() error = %v", err)
	}
	if res.Segments == 0 {
		t.Fatal("Trim() removed 0 segments, want > 0")
	}
	remaining := int64(0)
	for _, s := range l.segments {
		remaining += s.Size()
	}
	if remaining > total/2 {
		t.Fatalf("remaining size %d exceeds budget %d", remaining, total/2)
	}
	if l.NextOffset() != 40 {
		t.Fatalf("NextOffset() = %v, want 40 (offsets preserved)", l.NextOffset())
	}
	if len(l.segments) == 0 || l.segments[len(l.segments)-1] != l.active {
		t.Fatal("active segment was deleted by size-based trim")
	}
}

func TestTrimActiveProtected(t *testing.T) {
	cfg := testCfg()
	cfg.MaxSegmentBytes = 200
	_, l := newTestLog(t, cfg)
	now := time.Now()
	for i := 0; i < 10; i++ {
		appendAt(t, l, now.Add(-2*time.Hour), fmt.Sprintf("payload-%03d", i))
	}
	if len(l.segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(l.segments))
	}
	active := l.active

	// A nanosecond window deletes every sealed segment but never the active one.
	if _, err := l.Trim(time.Now(), retention.Policy{MaxAge: time.Nanosecond}); err != nil {
		t.Fatalf("Trim() error = %v", err)
	}
	if l.active != active {
		t.Fatal("active segment was replaced")
	}
	if len(l.segments) != 1 {
		t.Fatalf("segments after aggressive trim = %d, want 1 (active only)", len(l.segments))
	}
	if l.NextOffset() != 10 {
		t.Fatalf("NextOffset() = %v, want 10", l.NextOffset())
	}
	// Only the active segment's records survive; nothing fatal, no panics.
	recs := readAll(t, l, 0)
	if want := int(active.NextOffset() - active.Base()); len(recs) != want {
		t.Fatalf("records after aggressive trim = %d, want %d (active only)", len(recs), want)
	}
}

func mustTopicName(t *testing.T, s string) topic.Name {
	t.Helper()
	n, err := topic.NewName(s)
	if err != nil {
		t.Fatalf("NewName(%q) error = %v", s, err)
	}
	return n
}

func lastLogFile(t *testing.T, dir string) string {
	t.Helper()
	files := logFiles(t, dir)
	return files[len(files)-1]
}

func firstLogFile(t *testing.T, dir string) string {
	t.Helper()
	return logFiles(t, dir)[0]
}

func logFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out) // lexicographic order is offset order (zero-padded)
	return out
}
