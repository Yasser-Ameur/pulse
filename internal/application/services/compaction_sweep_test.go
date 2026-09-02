package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/storage"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

func durationOf(seconds int64) time.Duration { return time.Duration(seconds) * time.Second }

func TestBrokerSweepCompactsCompactTopics(t *testing.T) {
	b, _, factory, _ := newSweepBroker(t, 0)
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() { _ = b.Shutdown(ctx) }()

	compacted, err := b.CreateTopic(ctx, "compacted", topic.Config{Cleanup: topic.CleanupCompact}, 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	deleted, err := b.CreateTopic(ctx, "deleted", topic.Config{Cleanup: topic.CleanupDelete}, 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	for _, name := range []topic.Name{compacted.Name, deleted.Name} {
		if _, err := b.Publish(ctx, name, partition.ID(0), []message.Message{{Payload: []byte("x")}}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	b.sweep()

	compactedLog := b.partitionLog(t, factory, compacted.Name, partition.ID(0))
	compactedLog.mu.Lock()
	compactCalls := compactedLog.compactCalls
	compactedLog.mu.Unlock()
	if compactCalls != 1 {
		t.Fatalf("compact calls for compacted topic = %d, want 1", compactCalls)
	}

	deletedLog := b.partitionLog(t, factory, deleted.Name, partition.ID(0))
	deletedLog.mu.Lock()
	deletedCompactCalls, deletedTrimCalls := deletedLog.compactCalls, deletedLog.trimCalls
	deletedLog.mu.Unlock()
	if deletedCompactCalls != 0 {
		t.Fatalf("compact calls for delete-cleanup topic = %d, want 0", deletedCompactCalls)
	}
	// deleted has no retention limits configured, so Trim is skipped too.
	if deletedTrimCalls != 0 {
		t.Fatalf("trim calls for delete-cleanup topic with no retention policy = %d, want 0", deletedTrimCalls)
	}
}

func TestBrokerSweepReportsCompactionFailure(t *testing.T) {
	b, _, factory, _ := newSweepBroker(t, 0)
	logger := &fakeLogger{}
	b.logger = logger
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() { _ = b.Shutdown(ctx) }()

	compacted, err := b.CreateTopic(ctx, "compacted", topic.Config{Cleanup: topic.CleanupCompact}, 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := b.Publish(ctx, compacted.Name, partition.ID(0), []message.Message{{Payload: []byte("x")}}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	lg := b.partitionLog(t, factory, compacted.Name, partition.ID(0))
	lg.mu.Lock()
	lg.compactErr = errors.New("disk full")
	lg.mu.Unlock()

	b.sweep()

	logger.mu.Lock()
	defer logger.mu.Unlock()
	if !containsMsg(logger.warns, "compaction sweep failed") {
		t.Fatalf("warns logged = %v, want compaction sweep failed", logger.warns)
	}
}

func TestBrokerSweepLogsCompactionResult(t *testing.T) {
	b, _, factory, _ := newSweepBroker(t, 0)
	logger := &fakeLogger{}
	b.logger = logger
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() { _ = b.Shutdown(ctx) }()

	compacted, err := b.CreateTopic(ctx, "compacted", topic.Config{Cleanup: topic.CleanupCompact}, 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := b.Publish(ctx, compacted.Name, partition.ID(0), []message.Message{{Payload: []byte("x")}}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	lg := b.partitionLog(t, factory, compacted.Name, partition.ID(0))
	lg.mu.Lock()
	lg.compactResult = storage.CompactionResult{Segments: 1, BytesBefore: 100, BytesAfter: 40, TombstonesRemoved: 2}
	lg.mu.Unlock()

	b.sweep()

	logger.mu.Lock()
	defer logger.mu.Unlock()
	if !containsMsg(logger.infos, "compaction swept") {
		t.Fatalf("infos logged = %v, want compaction swept", logger.infos)
	}
}

// TestSweepIntervalPicksSmallerNonzero covers the shared maintenance loop's
// tick period (docs/compaction-design.md sec 8): retention and compaction run
// on the same goroutine, paced by whichever configured interval is smaller,
// with a disabled (zero) one never winning over an enabled one.
func TestSweepIntervalPicksSmallerNonzero(t *testing.T) {
	cases := []struct {
		name       string
		retention  int64
		compaction int64
		want       int64
	}{
		{"both zero", 0, 0, 0},
		{"only retention", 5, 0, 5},
		{"only compaction", 0, 7, 7},
		{"retention smaller", 3, 9, 3},
		{"compaction smaller", 9, 3, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := &Broker{
				retentionInterval:  durationOf(c.retention),
				compactionInterval: durationOf(c.compaction),
			}
			if got := b.sweepInterval(); got != durationOf(c.want) {
				t.Errorf("sweepInterval() = %v, want %v", got, durationOf(c.want))
			}
		})
	}
}
