// Package storage defines the Log port: the contract every partition log must
// satisfy.
//
// The domain reasons about logs, records, and offsets. Physical storage
// (segments, indexes, files) is an infrastructure concern and never appears
// here. This is the seam that keeps the storage engine replaceable.
package storage

import (
	"context"

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
)

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

	// Truncate removes all records at or after to, returning the log to the
	// state it would have had had those records never been appended.
	Truncate(to offset.Offset) error

	// Close flushes and releases all resources associated with the log.
	Close() error
}
