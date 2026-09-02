package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/pulse-stream/pulse/internal/application/services"
	"github.com/pulse-stream/pulse/internal/infrastructure/logging"
	"github.com/pulse-stream/pulse/internal/infrastructure/metrics"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/log"
	storagemeta "github.com/pulse-stream/pulse/internal/infrastructure/storage/metadata"
	"github.com/pulse-stream/pulse/internal/infrastructure/timeutil"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

// newTestApp assembles a real broker facade (metadata store + log factory on
// a temp dir) without any gRPC transport, so BrokerServer/PubSubServer can be
// exercised directly as thin adapters over it.
func newTestApp(t *testing.T) *services.Broker {
	t.Helper()
	dir := t.TempDir()
	meta, err := storagemeta.OpenPebble(dir + "/meta")
	require.NoError(t, err)
	factory := log.NewFactory(dir, log.Config{}, logging.NewNopLogger())
	app := services.NewBroker(services.BrokerOptions{
		MetadataStore: meta,
		LogFactory:    factory,
		Clock:         timeutil.SystemClock{},
		Logger:        logging.NewNopLogger(),
		Metrics:       metrics.NoopRecorder{},
		ListenAddr:    "127.0.0.1:0",
		Version:       "test",
		ReadLimit:     100,
		ReadMaxBytes:  1 << 20,
	})
	require.NoError(t, app.Start(context.Background()))
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })
	return app
}

func TestBrokerServerCreateListDeleteTopic(t *testing.T) {
	app := newTestApp(t)
	s := NewBrokerServer(app)
	ctx := context.Background()

	created, err := s.CreateTopic(ctx, &pulsepb.CreateTopicRequest{Name: "orders", Partitions: 3})
	require.NoError(t, err)
	require.Equal(t, "orders", created.Topic.Name)
	require.Equal(t, int32(3), created.Topic.Partitions)

	listed, err := s.ListTopics(ctx, &pulsepb.ListTopicsRequest{})
	require.NoError(t, err)
	require.Len(t, listed.Topics, 1)
	require.Equal(t, "orders", listed.Topics[0].Name)

	_, err = s.DeleteTopic(ctx, &pulsepb.DeleteTopicRequest{Name: "orders"})
	require.NoError(t, err)

	listed, err = s.ListTopics(ctx, &pulsepb.ListTopicsRequest{})
	require.NoError(t, err)
	require.Empty(t, listed.Topics)
}

func TestBrokerServerCreateTopicInvalidNameMapsError(t *testing.T) {
	app := newTestApp(t)
	s := NewBrokerServer(app)
	_, err := s.CreateTopic(context.Background(), &pulsepb.CreateTopicRequest{Name: "", Partitions: 1})
	require.Error(t, err)
}

func TestBrokerServerBrokerInfoReportsTopicCount(t *testing.T) {
	app := newTestApp(t)
	s := NewBrokerServer(app)
	ctx := context.Background()
	_, err := s.CreateTopic(ctx, &pulsepb.CreateTopicRequest{Name: "events", Partitions: 1})
	require.NoError(t, err)

	info, err := s.BrokerInfo(ctx, &pulsepb.BrokerInfoRequest{})
	require.NoError(t, err)
	require.Equal(t, int32(1), info.Topics)
	require.Equal(t, "test", info.Version)
	require.NotEmpty(t, info.ClusterId)
}

// fakeSubscribeStream implements pulsepb.PubSub_SubscribeServer (a
// grpc.ServerStreamingServer[SubscribeResponse]) by collecting sent responses
// in memory, so Subscribe can be driven without a real network transport.
type fakeSubscribeStream struct {
	ctx  context.Context
	recv []*pulsepb.SubscribeResponse
}

func (f *fakeSubscribeStream) Send(m *pulsepb.SubscribeResponse) error {
	f.recv = append(f.recv, m)
	return nil
}
func (f *fakeSubscribeStream) Context() context.Context     { return f.ctx }
func (f *fakeSubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeSubscribeStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeSubscribeStream) SetTrailer(metadata.MD)       {}
func (f *fakeSubscribeStream) SendMsg(m any) error          { return nil }
func (f *fakeSubscribeStream) RecvMsg(m any) error          { return nil }

func TestPubSubServerPublishSubscribeAck(t *testing.T) {
	app := newTestApp(t)
	bs := NewBrokerServer(app)
	ps := NewPubSubServer(app, timeutil.SystemClock{})
	ctx := context.Background()

	_, err := bs.CreateTopic(ctx, &pulsepb.CreateTopicRequest{Name: "orders", Partitions: 1})
	require.NoError(t, err)

	pubResp, err := ps.Publish(ctx, &pulsepb.PublishRequest{
		Topic:     "orders",
		Partition: 0,
		Messages:  []*pulsepb.Message{{Payload: []byte("hello")}},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{0}, pubResp.Offsets)

	stream := &fakeSubscribeStream{ctx: ctx}
	err = ps.Subscribe(&pulsepb.SubscribeRequest{
		Topic:     "orders",
		Partition: 0,
		Consumer:  "c1",
		Follow:    false,
	}, stream)
	require.NoError(t, err)
	require.NotEmpty(t, stream.recv)
	require.Equal(t, []byte("hello"), stream.recv[0].Records[0].Message.Payload)

	ackResp, err := ps.Ack(ctx, &pulsepb.AckRequest{Topic: "orders", Consumer: "c1", Partition: 0, Offset: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), ackResp.Cursor)
}

func TestPubSubServerPublishInvalidTopicMapsError(t *testing.T) {
	app := newTestApp(t)
	ps := NewPubSubServer(app, timeutil.SystemClock{})
	_, err := ps.Publish(context.Background(), &pulsepb.PublishRequest{Topic: "", Messages: []*pulsepb.Message{{Payload: []byte("x")}}})
	require.Error(t, err)
}

func TestBrokerServerDeleteTopicMissingMapsError(t *testing.T) {
	app := newTestApp(t)
	s := NewBrokerServer(app)
	_, err := s.DeleteTopic(context.Background(), &pulsepb.DeleteTopicRequest{Name: "missing"})
	require.Error(t, err)
}

func TestPubSubServerAckUnknownTopicMapsError(t *testing.T) {
	app := newTestApp(t)
	ps := NewPubSubServer(app, timeutil.SystemClock{})
	_, err := ps.Ack(context.Background(), &pulsepb.AckRequest{Topic: "missing", Consumer: "c1", Partition: 0, Offset: 0})
	require.Error(t, err)
}

func TestPubSubServerSubscribeInvalidSubscriptionMapsError(t *testing.T) {
	app := newTestApp(t)
	bs := NewBrokerServer(app)
	ps := NewPubSubServer(app, timeutil.SystemClock{})
	ctx := context.Background()
	_, err := bs.CreateTopic(ctx, &pulsepb.CreateTopicRequest{Name: "orders", Partitions: 1})
	require.NoError(t, err)

	// A consumer name containing "/" fails ID.Validate, so sub.Validate()
	// rejects the subscription before any read is attempted.
	stream := &fakeSubscribeStream{ctx: ctx}
	err = ps.Subscribe(&pulsepb.SubscribeRequest{Topic: "orders", Partition: 0, Consumer: "bad/name"}, stream)
	require.Error(t, err)
}
