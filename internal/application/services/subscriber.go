package services

import (
	"context"
	"time"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/storage"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

// Subscriber delivers ordered records from a partition log and tracks consumer
// cursors.
type Subscriber struct {
	registry     *LogRegistry
	store        ports.MetadataStore
	logger       ports.Logger
	metrics      ports.MetricsRecorder
	readLimit    int
	readMaxBytes int
}

// SubscriberOptions configures the read behavior of a Subscriber.
type SubscriberOptions struct {
	// ReadLimit bounds the number of records returned per read.
	ReadLimit int
	// ReadMaxBytes bounds the decoded payload bytes returned per read.
	ReadMaxBytes int
}

// NewSubscriber builds a Subscriber over the given registry and metadata store.
func NewSubscriber(registry *LogRegistry, store ports.MetadataStore, opts SubscriberOptions, logger ports.Logger, metrics ports.MetricsRecorder) *Subscriber {
	return &Subscriber{
		registry:     registry,
		store:        store,
		logger:       logger,
		metrics:      metrics,
		readLimit:    opts.ReadLimit,
		readMaxBytes: opts.ReadMaxBytes,
	}
}

// Subscribe streams records for sub, calling emit for each read batch. When
// sub.Follow is false it replays to the current end of log and returns nil;
// when true it blocks until the context is canceled and returns ctx.Err().
func (s *Subscriber) Subscribe(ctx context.Context, sub consumer.Subscription, emit func([]message.Record) error) error {
	if err := sub.Validate(); err != nil {
		return err
	}
	lg, ok := s.registry.Log(sub.Topic, sub.Partition)
	if !ok {
		return partition.ErrNotFound
	}

	pos, err := s.startPosition(ctx, sub, lg)
	if err != nil {
		return err
	}

	for {
		ch := lg.Notify()
		start := time.Now()
		records, err := lg.Read(ctx, pos, s.readLimit, s.readMaxBytes)
		if err != nil {
			return err
		}
		if len(records) > 0 {
			payloadBytes := 0
			for i := range records {
				payloadBytes += len(records[i].Message.Payload)
			}
			if err := emit(records); err != nil {
				return err
			}
			s.metrics.RecordConsume(len(records), payloadBytes)
			s.metrics.RecordConsumeLatency(time.Since(start))
			pos = records[len(records)-1].Offset + 1
			continue
		}
		if !sub.Follow {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// startPosition resolves the read position from the subscription: an explicit
// Start offset wins over the consumer cursor, which wins over the log start.
func (s *Subscriber) startPosition(ctx context.Context, sub consumer.Subscription, lg storage.Log) (offset.Offset, error) {
	if sub.Start != nil {
		start := *sub.Start
		if !start.Valid() {
			return offset.Invalid, offset.ErrInvalid
		}
		if start > lg.NextOffset() {
			return offset.Invalid, offset.ErrOutOfRange
		}
		return start, nil
	}
	if sub.Consumer != "" {
		cursor, ok, err := s.store.GetCursor(ctx, sub.Consumer, sub.Topic, sub.Partition)
		if err != nil {
			return offset.Invalid, err
		}
		if ok {
			if cursor > lg.NextOffset() {
				return offset.Invalid, offset.ErrOutOfRange
			}
			return cursor, nil
		}
	}
	return 0, nil
}

// Ack advances a consumer's stored cursor to o, where o is the NEXT offset the
// consumer wants to receive — one past the last record it processed, not that
// record's own offset. startPosition reads from the cursor inclusively, so
// acking the last processed offset redelivers it on resume. Acknowledgments
// are monotonic: a regression is ignored and the existing cursor is returned.
func (s *Subscriber) Ack(ctx context.Context, c consumer.ID, t topic.Name, id partition.ID, o offset.Offset) (offset.Offset, error) {
	if err := c.Validate(); err != nil {
		return offset.Invalid, err
	}
	if _, ok := s.registry.Topic(t); !ok {
		return offset.Invalid, topic.ErrNotFound
	}
	if _, ok := s.registry.Log(t, id); !ok {
		return offset.Invalid, partition.ErrNotFound
	}
	if !o.Valid() {
		return offset.Invalid, message.ErrInvalidMessage
	}
	current, ok, err := s.store.GetCursor(ctx, c, t, id)
	if err != nil {
		return offset.Invalid, err
	}
	if ok && o <= current {
		return current, nil
	}
	if err := s.store.SaveCursor(ctx, c, t, id, o); err != nil {
		return offset.Invalid, err
	}
	s.logger.Debug("advanced cursor",
		ports.Field{Key: "consumer", Value: c.String()},
		ports.Field{Key: logKeyTopic, Value: t.String()},
		ports.Field{Key: logKeyPartition, Value: id.Int32()},
		ports.Field{Key: "offset", Value: o.Int64()},
	)
	return o, nil
}
