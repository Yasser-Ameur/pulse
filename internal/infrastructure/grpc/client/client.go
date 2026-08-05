// Package client implements the internal gRPC client used by the CLI, examples,
// and integration tests (docs/Protocol.md §6).
//
// It translates between the domain model and the pulse.v1 wire types and
// exposes the subscribe stream through a callback so that all consumers of the
// stream share one path.
package client

import (
	"context"
	"errors"
	"io"

	"github.com/pulse-stream/pulse/internal/domain/broker"
	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// DefaultMaxMsgBytes matches the server default transport bound.
const DefaultMaxMsgBytes = 64 << 20

// Client is a thin pulse.v1 client exposing the domain model.
type Client struct {
	conn   *grpc.ClientConn
	broker pulsepb.BrokerClient
	pubsub pulsepb.PubSubClient
}

// Dial connects to the broker at addr.
func Dial(addr string, opts ...grpc.DialOption) (*Client, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(DefaultMaxMsgBytes), grpc.MaxCallSendMsgSize(DefaultMaxMsgBytes)),
	}
	dialOpts = append(dialOpts, opts...)
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		broker: pulsepb.NewBrokerClient(conn),
		pubsub: pulsepb.NewPubSubClient(conn),
	}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// CreateTopic creates a topic and returns its definition.
func (c *Client) CreateTopic(ctx context.Context, name string, cfg topic.Config, partitions int) (topic.Topic, error) {
	resp, err := c.broker.CreateTopic(ctx, &pulsepb.CreateTopicRequest{
		Name:       name,
		Config:     toPBTopicConfig(cfg),
		Partitions: int32(partitions),
	})
	if err != nil {
		return topic.Topic{}, err
	}
	return fromPBTopic(resp.Topic)
}

// DeleteTopic deletes a topic and its data.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	_, err := c.broker.DeleteTopic(ctx, &pulsepb.DeleteTopicRequest{Name: name})
	return err
}

// ListTopics returns the cluster's topics in name order.
func (c *Client) ListTopics(ctx context.Context) ([]topic.Topic, error) {
	resp, err := c.broker.ListTopics(ctx, &pulsepb.ListTopicsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]topic.Topic, 0, len(resp.Topics))
	for _, pb := range resp.Topics {
		t, err := fromPBTopic(pb)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// BrokerInfo returns the broker identity and lifecycle state.
func (c *Client) BrokerInfo(ctx context.Context) (broker.Broker, error) {
	resp, err := c.broker.BrokerInfo(ctx, &pulsepb.BrokerInfoRequest{})
	if err != nil {
		return broker.Broker{}, err
	}
	return broker.Broker{
		ClusterID: broker.ClusterID(resp.ClusterId),
		BrokerID:  broker.BrokerID(resp.BrokerId),
		NodeID:    broker.NodeID(resp.NodeId),
		Address:   resp.Address,
		State:     fromBrokerState(resp.State),
		Version:   resp.Version,
	}, nil
}

// Publish appends messages to a topic partition and returns one offset per
// message, aligned by index.
func (c *Client) Publish(ctx context.Context, name topic.Name, id partition.ID, msgs []message.Message) ([]offset.Offset, error) {
	pb := make([]*pulsepb.Message, 0, len(msgs))
	for i := range msgs {
		pb = append(pb, toPBMessage(msgs[i]))
	}
	resp, err := c.pubsub.Publish(ctx, &pulsepb.PublishRequest{
		Topic:     name.String(),
		Partition: id.Int32(),
		Messages:  pb,
	})
	if err != nil {
		return nil, err
	}
	out := make([]offset.Offset, len(resp.Offsets))
	for i, o := range resp.Offsets {
		out[i] = offset.Offset(o)
	}
	return out, nil
}

// Subscribe streams records for sub, invoking emit per batch. Follow=false
// replays and returns; Follow=true streams until ctx is canceled.
func (c *Client) Subscribe(ctx context.Context, sub consumer.Subscription, emit func(message.Record) error) error {
	req := &pulsepb.SubscribeRequest{
		Consumer:  sub.Consumer.String(),
		Topic:     sub.Topic.String(),
		Partition: sub.Partition.Int32(),
		Follow:    sub.Follow,
	}
	if sub.Start != nil {
		v := int64(*sub.Start)
		req.StartOffset = &v
	}
	stream, err := c.pubsub.Subscribe(ctx, req)
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
				return err
			}
			return err
		}
		for _, pb := range resp.Records {
			if err := emit(fromPBRecord(sub.Topic, pb)); err != nil {
				return err
			}
		}
	}
}

// Ack advances a consumer's stored cursor to offset o and returns the stored
// cursor.
func (c *Client) Ack(ctx context.Context, id consumer.ID, name topic.Name, p partition.ID, o offset.Offset) (offset.Offset, error) {
	resp, err := c.pubsub.Ack(ctx, &pulsepb.AckRequest{
		Consumer:  id.String(),
		Topic:     name.String(),
		Partition: p.Int32(),
		Offset:    o.Int64(),
	})
	if err != nil {
		return offset.Invalid, err
	}
	return offset.Offset(resp.Cursor), nil
}
