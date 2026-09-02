// Package ports defines the seams between the application layer and the
// world: metadata storage, log creation, time, logging, and metrics.
//
// Every concrete implementation (Pebble, zap, system clock, Prometheus) lives
// in the infrastructure layer and is injected at the composition root. The
// application depends only on these interfaces.
package ports

import (
	"context"
	"errors"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/broker"
	"github.com/Yasser-Ameur/pulse/internal/domain/consumer"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/storage"
	"github.com/Yasser-Ameur/pulse/internal/domain/timeutil"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

// Clock re-exports the domain time port so that services can depend on the
// ports package alone.
type Clock = timeutil.Clock

// ErrLogNotFound is returned by LogFactory.Open when a partition has no log
// yet. It is the application-level signal that missing storage should be
// recreated from metadata.
var ErrLogNotFound = errors.New("partition log not found")

// Logger is the minimal structured logging interface used across the broker.
type Logger interface {
	// Debug logs a message at debug level.
	Debug(msg string, fields ...Field)
	// Info logs a message at info level.
	Info(msg string, fields ...Field)
	// Warn logs a message at warn level.
	Warn(msg string, fields ...Field)
	// Error logs a message at error level.
	Error(msg string, fields ...Field)
	// With returns a child logger with the given fields attached.
	With(fields ...Field) Logger
}

// Field is a single structured logging attribute.
type Field struct {
	// Key is the attribute name.
	Key string
	// Value is the attribute value.
	Value any
}

// MetricsRecorder is the observability hook for the data plane. Phase 1 ships
// the Noop implementation; Prometheus and OpenTelemetry recorders become
// additional infrastructure adapters without touching the application.
type MetricsRecorder interface {
	// RecordPublish counts published records and their payload bytes.
	RecordPublish(records, bytes int)
	// RecordConsume counts delivered records and their payload bytes.
	RecordConsume(records, bytes int)
	// RecordPublishLatency records the end-to-end publish handler latency.
	RecordPublishLatency(d time.Duration)
	// RecordConsumeLatency records the subscribe read loop latency.
	RecordConsumeLatency(d time.Duration)
	// RecordBytesWritten accounts bytes durably written to the data plane.
	RecordBytesWritten(n int64)
	// RecordBytesRead accounts bytes read from the data plane.
	RecordBytesRead(n int64)
}

// MetadataStore is the metadata plane: durable broker state that is not event
// data. Implementations must be safe for concurrent use and durable on success.
//
// The interface is deliberately explicit rather than generic so that a future
// log-compacted internal-topic implementation (Kafka-style __consumer_offsets)
// can satisfy it without reinterpretation.
type MetadataStore interface {
	// CreateTopic persists a new topic definition. It returns topic.ErrAlreadyExists
	// if a topic with the same name already exists.
	CreateTopic(ctx context.Context, t topic.Topic) error
	// GetTopic returns the topic definition. found is false when the topic
	// does not exist.
	GetTopic(ctx context.Context, name topic.Name) (topic.Topic, bool, error)
	// DeleteTopic removes a topic definition. It returns topic.ErrNotFound if
	// the topic does not exist.
	DeleteTopic(ctx context.Context, name topic.Name) error
	// ListTopics returns all topic definitions in name order.
	ListTopics(ctx context.Context) ([]topic.Topic, error)

	// SaveCursor advances a consumer's stored position. The store accepts any
	// offset; monotonicity is enforced by the application layer.
	SaveCursor(ctx context.Context, c consumer.ID, t topic.Name, p partition.ID, o offset.Offset) error
	// GetCursor returns a consumer's stored position. found is false when the
	// consumer has never acknowledged anything for this partition.
	GetCursor(ctx context.Context, c consumer.ID, t topic.Name, p partition.ID) (offset.Offset, bool, error)

	// CreateCluster persists the cluster identity. It is idempotent.
	CreateCluster(ctx context.Context, id broker.ClusterID) error
	// ClusterID returns the persisted cluster identity. found is false when no
	// cluster has been created yet.
	ClusterID(ctx context.Context) (broker.ClusterID, bool, error)
	// CreateBroker persists a broker identity. It is idempotent.
	CreateBroker(ctx context.Context, id broker.BrokerID) error
	// BrokerID returns the persisted broker identity. found is false when no
	// broker has been created yet.
	BrokerID(ctx context.Context) (broker.BrokerID, bool, error)

	// Close releases all resources held by the store.
	Close() error
}

// LogFactory creates, opens, and deletes the persistent log for a partition.
type LogFactory interface {
	// Create makes a new empty log for the partition, creating its directory.
	Create(ctx context.Context, name topic.Name, id partition.ID) (storage.Log, error)
	// Open opens the existing log for the partition. It returns
	// filesystem.ErrNotFound (via the infrastructure layer) when the partition
	// has no log yet.
	Open(ctx context.Context, name topic.Name, id partition.ID) (storage.Log, error)
	// Delete removes the partition log and all of its data.
	Delete(ctx context.Context, name topic.Name, id partition.ID) error
}
