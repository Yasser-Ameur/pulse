package services

import (
	"context"
	"time"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

// Publisher appends messages to a partition log and assigns their identities.
type Publisher struct {
	registry *LogRegistry
	clock    ports.Clock
	logger   ports.Logger
	metrics  ports.MetricsRecorder
}

// NewPublisher builds a Publisher over the given registry.
func NewPublisher(registry *LogRegistry, clock ports.Clock, logger ports.Logger, metrics ports.MetricsRecorder) *Publisher {
	return &Publisher{
		registry: registry,
		clock:    clock,
		logger:   logger,
		metrics:  metrics,
	}
}

// Publish validates messages against the topic configuration, assigns event
// identifiers, timestamps, and offsets, and durably appends a single batch.
// It returns one offset per input message, aligned by index.
func (p *Publisher) Publish(ctx context.Context, t topic.Topic, id partition.ID, msgs []message.Message) ([]offset.Offset, error) {
	if len(msgs) == 0 {
		return nil, message.ErrEmptyBatch
	}
	if len(msgs) > message.MaxBatchRecords {
		return nil, message.ErrBatchTooLarge
	}
	lg, ok := p.registry.Log(t.Name, id)
	if !ok {
		return nil, partition.ErrNotFound
	}

	now := p.clock.Now()
	batch := &message.RecordBatch{
		FirstTimestamp: now,
		LastTimestamp:  now,
		Records:        make([]message.Record, 0, len(msgs)),
	}
	totalBytes := 0
	for i := range msgs {
		m := msgs[i]
		if err := m.Validate(t.Config.MaxMessageBytes); err != nil {
			return nil, err
		}
		if m.EventID.Zero() {
			m.EventID = message.NewEventID(now)
		}
		batch.Records = append(batch.Records, message.Record{
			Offset:    offset.Invalid,
			Timestamp: now,
			Message:   m,
		})
		totalBytes += len(m.Payload)
	}

	start := time.Now()
	base, err := lg.Append(ctx, batch)
	if err != nil {
		return nil, err
	}
	p.metrics.RecordPublishLatency(time.Since(start))
	p.metrics.RecordPublish(len(msgs), totalBytes)
	p.metrics.RecordBytesWritten(int64(totalBytes))

	offsets := make([]offset.Offset, len(batch.Records))
	for i, r := range batch.Records {
		offsets[i] = r.Offset
	}
	p.logger.Debug("published batch",
		ports.Field{Key: logKeyTopic, Value: t.Name.String()},
		ports.Field{Key: logKeyPartition, Value: id.Int32()},
		ports.Field{Key: "base_offset", Value: base.Int64()},
		ports.Field{Key: "records", Value: len(batch.Records)},
	)
	return offsets, nil
}
