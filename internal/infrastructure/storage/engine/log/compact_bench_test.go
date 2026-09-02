package log

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

// benchKeys returns n distinct keys for a duplication-ratio benchmark.
func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

// buildCompactionBenchLog creates a log with `segments` sealed segments, each
// holding one record per key in `keys` repeated `perKeyPerSegment` times, so
// only the last occurrence of each key across the whole log survives
// compaction. Returns the log plus the root/name/id needed to reopen it.
func buildCompactionBenchLog(b *testing.B, segments, perKeyPerSegment int, keys []string) (l *Log, root string, name topic.Name, id partition.ID) {
	b.Helper()
	root = b.TempDir()
	name, err := topic.NewName("bench")
	if err != nil {
		b.Fatalf("NewName() error = %v", err)
	}
	id = partition.ID(0)
	cfg := Config{MaxSegmentBytes: 1 << 30, IndexInterval: 4096, SyncMode: SyncEveryWrite, TombstoneRetention: time.Hour}
	l, err = CreateLog(root, name, id, cfg, nil)
	if err != nil {
		b.Fatalf("CreateLog() error = %v", err)
	}
	now := time.Unix(1700000000, 0).UTC()
	for s := 0; s < segments; s++ {
		for i := 0; i < perKeyPerSegment; i++ {
			for _, key := range keys {
				batch := &message.RecordBatch{
					FirstTimestamp: now,
					LastTimestamp:  now,
					Records: []message.Record{{
						Timestamp: now,
						Message:   message.Message{Key: key, Payload: []byte("payload-value-of-realistic-size")},
					}},
				}
				if _, err := l.Append(context.Background(), batch); err != nil {
					b.Fatalf("Append() error = %v", err)
				}
			}
		}
		l.mu.Lock()
		err := l.rotate()
		l.mu.Unlock()
		if err != nil {
			b.Fatalf("rotate() error = %v", err)
		}
	}
	return l, root, name, id
}

// BenchmarkCompactThroughput measures the time and bytes moved by one
// Log.Compact call at various duplication ratios (records per key per
// segment).
func BenchmarkCompactThroughput(b *testing.B) {
	for _, dup := range []int{1, 5, 25} {
		b.Run(fmt.Sprintf("dup=%d", dup), func(b *testing.B) {
			keys := benchKeys(20)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				l, _, _, _ := buildCompactionBenchLog(b, 20, dup, keys)
				b.StartTimer()
				res, err := l.Compact(context.Background())
				if err != nil {
					b.Fatalf("Compact() error = %v", err)
				}
				b.StopTimer()
				b.SetBytes(res.BytesBefore)
				_ = l.Close()
			}
		})
	}
}

// BenchmarkCompactAllocs reports the allocations of one Log.Compact call
// (run with -benchmem).
func BenchmarkCompactAllocs(b *testing.B) {
	keys := benchKeys(20)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		l, _, _, _ := buildCompactionBenchLog(b, 20, 10, keys)
		b.StartTimer()
		if _, err := l.Compact(context.Background()); err != nil {
			b.Fatalf("Compact() error = %v", err)
		}
		b.StopTimer()
		_ = l.Close()
	}
}

// BenchmarkRecoverAfterCompaction measures OpenLog's recovery time over an
// already-compacted (sparse-offset) log.
func BenchmarkRecoverAfterCompaction(b *testing.B) {
	keys := benchKeys(20)
	l, root, name, id := buildCompactionBenchLog(b, 20, 10, keys)
	if _, err := l.Compact(context.Background()); err != nil {
		b.Fatalf("Compact() error = %v", err)
	}
	cfg := Config{TombstoneRetention: time.Hour}
	if err := l.Close(); err != nil {
		b.Fatalf("Close() error = %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := OpenLog(root, name, id, cfg, nil)
		if err != nil {
			b.Fatalf("OpenLog() error = %v", err)
		}
		b.StopTimer()
		_ = r.Close()
		b.StartTimer()
	}
}

// BenchmarkLookupAfterCompaction measures Read from a dropped offset (a hole,
// resolved to the next survivor) versus a surviving offset.
func BenchmarkLookupAfterCompaction(b *testing.B) {
	keys := benchKeys(20)
	l, _, _, _ := buildCompactionBenchLog(b, 20, 10, keys)
	if _, err := l.Compact(context.Background()); err != nil {
		b.Fatalf("Compact() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	ctx := context.Background()

	b.Run("hole", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := l.Read(ctx, 0, 1, 0); err != nil {
				b.Fatalf("Read() error = %v", err)
			}
		}
	})
	b.Run("survivor", func(b *testing.B) {
		last := l.NextOffset() - 1
		for i := 0; i < b.N; i++ {
			if _, err := l.Read(ctx, last, 1, 0); err != nil {
				b.Fatalf("Read() error = %v", err)
			}
		}
	})
}

// BenchmarkAppendBeforeAfterCompaction measures publish latency with a
// compaction running concurrently, proving the writer lock is only held for
// the short swap and not the whole pass.
func BenchmarkAppendBeforeAfterCompaction(b *testing.B) {
	keys := benchKeys(20)
	l, _, _, _ := buildCompactionBenchLog(b, 20, 10, keys)
	defer func() { _ = l.Close() }()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()

	b.Run("without_compaction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			batch := &message.RecordBatch{
				FirstTimestamp: now, LastTimestamp: now,
				Records: []message.Record{{Timestamp: now, Message: message.Message{Payload: []byte("x")}}},
			}
			if _, err := l.Append(ctx, batch); err != nil {
				b.Fatalf("Append() error = %v", err)
			}
		}
	})

	b.Run("during_compaction", func(b *testing.B) {
		done := make(chan struct{})
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					_, _ = l.Compact(ctx)
				}
			}
		}()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batch := &message.RecordBatch{
				FirstTimestamp: now, LastTimestamp: now,
				Records: []message.Record{{Timestamp: now, Message: message.Message{Payload: []byte("x")}}},
			}
			if _, err := l.Append(ctx, batch); err != nil {
				b.Fatalf("Append() error = %v", err)
			}
		}
		b.StopTimer()
		close(done)
	})
}
