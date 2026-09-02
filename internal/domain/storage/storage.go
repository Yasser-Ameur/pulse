// Package storage defines the Log port: the contract every partition log must
// satisfy.
//
// The domain reasons about logs, records, and offsets. Physical storage
// (segments, indexes, files) is an infrastructure concern and never appears
// here. This is the seam that keeps the storage engine replaceable.
package storage

import (
	"context"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/retention"
)

// CompactionResult reports what one call to Log.Compact did.
type CompactionResult struct {
	// Segments is the number of sealed segments rewritten.
	Segments int
	// BytesBefore is the total size of the rewritten segments before
	// compaction.
	BytesBefore int64
	// BytesAfter is their total size after compaction.
	BytesAfter int64
	// TombstonesRemoved is the number of expired tombstones dropped.
	TombstonesRemoved int
}

// Log is an append-only, durable, ordered sequence of record batches.
//
// Implementations must guarantee that an acknowledged append is durable, that
// reads observe a prefix of the log, and that offsets are assigned
// monotonically under the append lock. Log is safe for concurrent use.
type Log interface {
	// Append durably persists batch and assigns it consecutive offsets
	// starting at the log's next offset. On success, batch.BaseOffset and each
	// record's Offset are populated and the returned offset is the batch's
	// base offset.
	Append(ctx context.Context, batch *message.RecordBatch) (offset.Offset, error)

	// Read returns up to limit records at or after from, bounded by maxBytes
	// of decoded payload. The returned slice is empty when from is at or past
	// the end of the log or when ctx is canceled.
	Read(ctx context.Context, from offset.Offset, limit int, maxBytes int) ([]message.Record, error)

	// NextOffset returns the offset the next append will receive. This is the
	// log's end (LEO).
	NextOffset() offset.Offset

	// Notify returns a channel that is closed whenever data is appended.
	// Callers wait on it after an empty read and call Notify again to obtain
	// the replacement channel. It is never closed on log shutdown.
	Notify() <-chan struct{}

	// Sync flushes all buffered data to stable storage.
	Sync() error

	// Trim applies the retention policy at the given wall-clock time, deleting
	// sealed segments that fall entirely outside the retention window (age or
	// total bytes). The active segment is never deleted. Offsets of surviving
	// records are unchanged.
	Trim(now time.Time, policy retention.Policy) (retention.TrimResult, error)

	// Truncate removes all records at or after to, returning the log to the
	// state it would have had had those records never been appended.
	Truncate(to offset.Offset) error

	// Compact deduplicates the sealed segments of a compacted log, keeping
	// only the newest record per key (docs/compaction-design.md). It never
	// touches the active segment and never changes offsets or the log's end.
	// It is safe for concurrent use and returns a zero result when there is
	// nothing to do or another call is already in progress.
	Compact(ctx context.Context) (CompactionResult, error)

	// Close flushes and releases all resources associated with the log.
	Close() error
}
