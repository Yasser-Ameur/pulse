package client

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/Yasser-Ameur/pulse/pkg/api/pulse/v1/pulsepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultCallTimeout is applied to every unary call when the caller's
// context carries no deadline.
const DefaultCallTimeout = 30 * time.Second

// DefaultMaxMsgBytes is the default gRPC message size bound, matching the
// server's own transport default.
const DefaultMaxMsgBytes = 64 << 20

type options struct {
	tlsConfig   *tls.Config
	dialOpts    []grpc.DialOption
	callTimeout time.Duration
	maxMsgBytes int
}

// Option configures a Client at Dial time.
type Option func(*options)

// WithTLS dials using cfg instead of an insecure connection.
func WithTLS(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithDialOptions appends raw grpc.DialOptions, applied after the client's
// own transport and message-size options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dialOpts = append(o.dialOpts, opts...) }
}

// WithCallTimeout overrides DefaultCallTimeout.
func WithCallTimeout(d time.Duration) Option {
	return func(o *options) { o.callTimeout = d }
}

// WithMaxMsgBytes overrides DefaultMaxMsgBytes for both send and receive.
func WithMaxMsgBytes(n int) Option {
	return func(o *options) { o.maxMsgBytes = n }
}

// WithToken attaches token as per-RPC credentials, sent as gRPC metadata key
// "authorization" with value "Bearer <token>" on every call. Its
// RequireTransportSecurity is false, so it works over a plaintext connection
// too; use it only when the broker is reached over a trusted network or
// combine it with WithTLS. A missing or wrong token surfaces as
// ErrUnauthenticated.
func WithToken(token string) Option {
	return func(o *options) { o.dialOpts = append(o.dialOpts, grpc.WithPerRPCCredentials(tokenCreds(token))) }
}

// tokenCreds implements credentials.PerRPCCredentials for WithToken.
type tokenCreds string

func (t tokenCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}

func (t tokenCreds) RequireTransportSecurity() bool { return false }

// Client is a pulse.v1 client.
type Client struct {
	conn        *grpc.ClientConn
	broker      pulsepb.BrokerClient
	pubsub      pulsepb.PubSubClient
	callTimeout time.Duration
}

// Dial connects to the broker at addr.
func Dial(addr string, opts ...Option) (*Client, error) {
	o := options{
		callTimeout: DefaultCallTimeout,
		maxMsgBytes: DefaultMaxMsgBytes,
	}
	for _, opt := range opts {
		opt(&o)
	}

	creds := insecure.NewCredentials()
	if o.tlsConfig != nil {
		creds = credentials.NewTLS(o.tlsConfig)
	}
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(o.maxMsgBytes),
			grpc.MaxCallSendMsgSize(o.maxMsgBytes),
		),
	}
	dialOpts = append(dialOpts, o.dialOpts...)

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:        conn,
		broker:      pulsepb.NewBrokerClient(conn),
		pubsub:      pulsepb.NewPubSubClient(conn),
		callTimeout: o.callTimeout,
	}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// unaryCtx applies the client's call timeout to ctx when ctx has no deadline
// of its own.
func (c *Client) unaryCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.callTimeout)
}

// CreateTopic creates a topic and returns its definition.
func (c *Client) CreateTopic(ctx context.Context, name string, cfg TopicConfig) (Topic, error) {
	ctx, cancel := c.unaryCtx(ctx)
	defer cancel()
	resp, err := c.broker.CreateTopic(ctx, &pulsepb.CreateTopicRequest{
		Name:       name,
		Config:     toPBTopicConfig(cfg),
		Partitions: int32(cfg.Partitions),
	})
	if err != nil {
		return Topic{}, wrapErr(err)
	}
	return fromPBTopic(resp.Topic), nil
}

// DeleteTopic deletes a topic and its data.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	ctx, cancel := c.unaryCtx(ctx)
	defer cancel()
	_, err := c.broker.DeleteTopic(ctx, &pulsepb.DeleteTopicRequest{Name: name})
	return wrapErr(err)
}

// ListTopics returns the cluster's topics in name order.
func (c *Client) ListTopics(ctx context.Context) ([]Topic, error) {
	ctx, cancel := c.unaryCtx(ctx)
	defer cancel()
	resp, err := c.broker.ListTopics(ctx, &pulsepb.ListTopicsRequest{})
	if err != nil {
		return nil, wrapErr(err)
	}
	out := make([]Topic, 0, len(resp.Topics))
	for _, pb := range resp.Topics {
		out = append(out, fromPBTopic(pb))
	}
	return out, nil
}

// Info returns the broker identity and lifecycle state.
func (c *Client) Info(ctx context.Context) (BrokerInfo, error) {
	ctx, cancel := c.unaryCtx(ctx)
	defer cancel()
	resp, err := c.broker.BrokerInfo(ctx, &pulsepb.BrokerInfoRequest{})
	if err != nil {
		return BrokerInfo{}, wrapErr(err)
	}
	return BrokerInfo{
		ClusterID: resp.ClusterId,
		BrokerID:  resp.BrokerId,
		NodeID:    resp.NodeId,
		Address:   resp.Address,
		State:     brokerStateName(resp.State),
		Version:   resp.Version,
	}, nil
}

// Ack advances a consumer's stored cursor to next and returns the stored
// cursor. next is the offset one past the last record the consumer finished
// processing, not that record's own offset.
func (c *Client) Ack(ctx context.Context, consumer, topic string, partition int32, next int64) (int64, error) {
	ctx, cancel := c.unaryCtx(ctx)
	defer cancel()
	resp, err := c.pubsub.Ack(ctx, &pulsepb.AckRequest{
		Consumer:  consumer,
		Topic:     topic,
		Partition: partition,
		Offset:    next,
	})
	if err != nil {
		return 0, wrapErr(err)
	}
	return resp.Cursor, nil
}
