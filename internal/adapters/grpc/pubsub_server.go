package grpc

import (
	"context"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/application/services"
	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PubSubServer implements the pulse.v1 PubSub data-plane service.
type PubSubServer struct {
	pulsepb.UnimplementedPubSubServer
	app   *services.Broker
	clock ports.Clock
}

// NewPubSubServer builds a PubSubServer delegating to the application facade.
// clock must be the same clock injected into the broker so that reported batch
// timestamps match the broker-assigned ones.
func NewPubSubServer(app *services.Broker, clock ports.Clock) *PubSubServer {
	return &PubSubServer{app: app, clock: clock}
}

// Publish implements pulsepb.PubSubServer.
func (s *PubSubServer) Publish(ctx context.Context, req *pulsepb.PublishRequest) (*pulsepb.PublishResponse, error) {
	name, err := topic.NewName(req.Topic)
	if err != nil {
		return nil, mapError(err)
	}
	msgs := make([]message.Message, 0, len(req.Messages))
	for _, pb := range req.Messages {
		m, err := toMessage(pb)
		if err != nil {
			return nil, mapError(err)
		}
		msgs = append(msgs, m)
	}
	offsets, err := s.app.Publish(ctx, name, partition.ID(req.Partition), msgs)
	if err != nil {
		return nil, mapError(err)
	}
	resp := &pulsepb.PublishResponse{
		Topic:            req.Topic,
		Partition:        req.Partition,
		Offsets:          make([]int64, len(offsets)),
		FirstTimestampMs: s.clock.Now().UnixMilli(),
	}
	for i, o := range offsets {
		resp.Offsets[i] = o.Int64()
	}
	return resp, nil
}

// Subscribe implements the server-streaming pulsepb.PubSubServer.
func (s *PubSubServer) Subscribe(req *pulsepb.SubscribeRequest, stream pulsepb.PubSub_SubscribeServer) error {
	name, err := topic.NewName(req.Topic)
	if err != nil {
		return mapError(err)
	}
	sub := consumer.Subscription{
		Consumer:  consumer.ID(req.Consumer),
		Topic:     name,
		Partition: partition.ID(req.Partition),
		Start:     toStartOffset(req.StartOffset),
		Follow:    req.Follow,
	}
	if err := sub.Validate(); err != nil {
		return mapError(err)
	}

	// No intermediate buffering (Concurrency.md §3): records are decoded under
	// a short read lock by the subscriber and written straight to the stream.
	emit := func(records []message.Record) error {
		resp := &pulsepb.SubscribeResponse{Records: make([]*pulsepb.Record, 0, len(records))}
		for i := range records {
			resp.Records = append(resp.Records, toPBRecord(name, req.Partition, records[i]))
		}
		return stream.Send(resp)
	}

	if err := s.app.Subscribe(stream.Context(), sub, emit); err != nil {
		if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
			return err
		}
		return mapError(err)
	}
	return nil
}

// Ack implements pulsepb.PubSubServer.
func (s *PubSubServer) Ack(ctx context.Context, req *pulsepb.AckRequest) (*pulsepb.AckResponse, error) {
	name, err := topic.NewName(req.Topic)
	if err != nil {
		return nil, mapError(err)
	}
	cursor, err := s.app.Ack(ctx, consumer.ID(req.Consumer), name, partition.ID(req.Partition), offset.Offset(req.Offset))
	if err != nil {
		return nil, mapError(err)
	}
	return &pulsepb.AckResponse{Cursor: cursor.Int64()}, nil
}

var _ pulsepb.PubSubServer = (*PubSubServer)(nil)
