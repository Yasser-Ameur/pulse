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

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/filesystem"
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

	name, _ := topic.NewName("orders")
	if _, err := OpenLog(root, name, partition.ID(0), testCfg(), nil); err == nil {
		t.Fatal("OpenLog() error = nil, want fatal corruption error")
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
