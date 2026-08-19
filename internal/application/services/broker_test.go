package services

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/domain/broker"
	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/retention"
	"github.com/pulse-stream/pulse/internal/domain/storage"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

func newTestBroker(t *testing.T) (*Broker, *fakeStore, *fakeFactory, *LogRegistry) {
	t.Helper()
	store := newFakeStore()
	factory := newFakeFactory()
	b := NewBroker(BrokerOptions{
		MetadataStore: store,
		LogFactory:    factory,
		Clock:         &fakeClock{now: timeNow()},
		Logger:        &fakeLogger{},
		Metrics:       fakeMetrics{},
		ListenAddr:    "127.0.0.1:9090",
		Version:       "test",
		ReadLimit:     100,
		ReadMaxBytes:  1 << 20,
	})
	return b, store, factory, b.registry
}

func TestBrokerStartAssignsIdentity(t *testing.T) {
	b, store, _, _ := newTestBroker(t)
	if b.State() != broker.StateStarting {
		t.Fatalf("State() = %v, want Starting", b.State())
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if b.State() != broker.StateRunning {
		t.Fatalf("State() = %v, want Running", b.State())
	}
	clusterID, ok, err := store.ClusterID(context.Background())
	if err != nil || !ok || clusterID == "" {
		t.Fatalf("cluster id persisted = %q, %v, %v", clusterID, ok, err)
	}
	brokerID, ok, err := store.BrokerID(context.Background())
	if err != nil || !ok || brokerID == "" {
		t.Fatalf("broker id persisted = %q, %v, %v", brokerID, ok, err)
	}
	info := b.BrokerInfo()
	if info.ClusterID != clusterID || info.BrokerID != brokerID {
		t.Fatalf("BrokerInfo() = %+v, want cluster %q broker %q", info, clusterID, brokerID)
	}
	if info.State != broker.StateRunning {
		t.Fatalf("BrokerInfo().State = %v, want Running", info.State)
	}
	if info.Address != "127.0.0.1:9090" || info.Version != "test" {
		t.Fatalf("BrokerInfo() address/version = %q/%q, want advertised values", info.Address, info.Version)
	}
}

func TestBrokerStartPersistsIdentityAcrossRestarts(t *testing.T) {
	b, store, _, _ := newTestBroker(t)
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	wantCluster, _, _ := store.ClusterID(context.Background())
	wantBroker, _, _ := store.BrokerID(context.Background())

	b2 := NewBroker(BrokerOptions{
		MetadataStore: store,
		LogFactory:    newFakeFactory(),
		Clock:         &fakeClock{now: timeNow()},
		Logger:        &fakeLogger{},
		Metrics:       fakeMetrics{},
		ReadLimit:     100,
		ReadMaxBytes:  1 << 20,
	})
	if err := b2.Start(context.Background()); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	cluster, _, _ := store.ClusterID(context.Background())
	id, _, _ := store.BrokerID(context.Background())
	if cluster != wantCluster || id != wantBroker {
		t.Fatalf("identity changed across restart: %q/%q vs %q/%q", cluster, id, wantCluster, wantBroker)
	}
}

func TestBrokerStartRecreatesMissingLogs(t *testing.T) {
	store := newFakeStore()
	factory := newFakeFactory()
	ctx := context.Background()
	// Metadata exists for a topic but the storage was lost (e.g. data dir
	// removed). Startup must recreate the partition log.
	if err := store.CreateTopic(ctx, testTopic(mustName(t, "orders"), 1)); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	b := NewBroker(BrokerOptions{
		MetadataStore: store,
		LogFactory:    factory,
		Clock:         &fakeClock{now: timeNow()},
		Logger:        &fakeLogger{},
		Metrics:       fakeMetrics{},
		ReadLimit:     100,
		ReadMaxBytes:  1 << 20,
	})
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if len(factory.created) != 1 {
		t.Fatalf("factory.created = %v, want the recreated partition log", factory.created)
	}
	if _, ok := b.registry.Topic(mustName(t, "orders")); !ok {
		t.Fatal("topic not registered after recovery")
	}
}

func TestBrokerOperationsRequireRunning(t *testing.T) {
	b, _, _, _ := newTestBroker(t)
	ctx := context.Background()
	name := mustName(t, "orders")

	if _, err := b.Publish(ctx, name, partition.ID(0), []message.Message{{Payload: []byte("x")}}); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Publish() error = %v, want ErrNotRunning", err)
	}
	if err := b.Subscribe(ctx, subscriptionFor(name), func([]message.Record) error { return nil }); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Subscribe() error = %v, want ErrNotRunning", err)
	}
	if _, err := b.Ack(ctx, "w", name, partition.ID(0), offset.Offset(1)); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Ack() error = %v, want ErrNotRunning", err)
	}
	if _, err := b.CreateTopic(ctx, "orders", topic.DefaultConfig(), 1); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("CreateTopic() error = %v, want ErrNotRunning", err)
	}
	if err := b.DeleteTopic(ctx, "orders"); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("DeleteTopic() error = %v, want ErrNotRunning", err)
	}
}

func TestBrokerEndToEndPublishSubscribeAck(t *testing.T) {
	b, _, _, _ := newTestBroker(t)
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	tpc, err := b.CreateTopic(ctx, "orders", topic.DefaultConfig(), 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	offsets, err := b.Publish(ctx, tpc.Name, partition.ID(0), []message.Message{
		{Payload: []byte("a")},
		{Payload: []byte("b")},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(offsets) != 2 || offsets[0] != offset.Offset(0) || offsets[1] != offset.Offset(1) {
		t.Fatalf("offsets = %v, want [0 1]", offsets)
	}

	var got []message.Record
	if err := b.Subscribe(ctx, subscriptionFor(tpc.Name), func(recs []message.Record) error {
		got = append(got, recs...)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if len(got) != 2 || string(got[1].Message.Payload) != "b" {
		t.Fatalf("delivered = %v, want [a b]", got)
	}

	acked, err := b.Ack(ctx, "worker", tpc.Name, partition.ID(0), offsets[1])
	if err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if acked != offset.Offset(1) {
		t.Fatalf("Ack() = %v, want 1", acked)
	}
}

func TestBrokerShutdownLifecycle(t *testing.T) {
	b, store, _, _ := newTestBroker(t)
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := b.CreateTopic(ctx, "orders", topic.DefaultConfig(), 1); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if b.State() != broker.StateStopped {
		t.Fatalf("State() = %v, want Stopped", b.State())
	}
	if !store.closed {
		t.Fatal("metadata store not closed on shutdown")
	}
	for _, lg := range b.registry.Logs() {
		if fl, ok := lg.(*fakeLog); ok && !fl.closed {
			t.Fatal("log not closed on shutdown")
		}
	}

	// Idempotent.
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
	// Cannot restart a stopped broker.
	if err := b.Start(ctx); !errors.Is(err, broker.ErrInvalidTransition) {
		t.Fatalf("Start() after shutdown error = %v, want ErrInvalidTransition", err)
	}
}

func TestBrokerRejectsUnknownTopicOnPublish(t *testing.T) {
	b, _, _, _ := newTestBroker(t)
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := b.Publish(ctx, mustName(t, "ghost"), partition.ID(0), []message.Message{{Payload: []byte("x")}}); !errors.Is(err, topic.ErrNotFound) {
		t.Fatalf("Publish() error = %v, want topic.ErrNotFound", err)
	}
}

func TestBrokerLogFactoryInjectability(t *testing.T) {
	// The composition root must be able to pass any LogFactory implementation;
	// verify the interface contract compiles against a concrete fake.
	var _ ports.LogFactory = newFakeFactory()
	var _ storage.Log = newFakeLog()
}

func newSweepBroker(t *testing.T, interval time.Duration) (*Broker, *fakeStore, *fakeFactory, *fakeClock) {
	t.Helper()
	store := newFakeStore()
	factory := newFakeFactory()
	clock := &fakeClock{now: timeNow()}
	b := NewBroker(BrokerOptions{
		MetadataStore:     store,
		LogFactory:        factory,
		Clock:             clock,
		Logger:            &fakeLogger{},
		Metrics:           fakeMetrics{},
		RetentionInterval: interval,
	})
	return b, store, factory, clock
}

func (b *Broker) partitionLog(t *testing.T, factory *fakeFactory, name topic.Name, id partition.ID) *fakeLog {
	t.Helper()
	lg, ok := factory.logs[partitionKey{topicName: name, partition: id}]
	if !ok {
		t.Fatalf("log for %s/%d not created", name, id)
	}
	return lg
}

func TestBrokerSweepTrimsEligibleTopics(t *testing.T) {
	b, store, factory, _ := newSweepBroker(t, 10*time.Millisecond)
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() { _ = b.Shutdown(ctx) }()

	tpc, err := b.CreateTopic(ctx, "orders", topic.Config{
		Cleanup:   topic.CleanupDelete,
		Retention: retention.Policy{MaxAge: 5 * time.Minute},
	}, 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := b.Publish(ctx, tpc.Name, partition.ID(0), []message.Message{{Payload: []byte("x")}}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	b.sweep()
	lg := b.partitionLog(t, factory, tpc.Name, partition.ID(0))
	lg.mu.Lock()
	calls, policy := lg.trimCalls, lg.lastPolicy
	lg.mu.Unlock()
	if calls != 1 {
		t.Fatalf("partition trim calls = %d, want 1", calls)
	}
	if policy.MaxAge != 5*time.Minute {
		t.Fatalf("trim policy MaxAge = %v, want 5m", policy.MaxAge)
	}
	// Topic metadata is persisted; ensure the store path was exercised too.
	if _, _, err := store.GetTopic(ctx, tpc.Name); err != nil {
		t.Fatalf("GetTopic() error = %v", err)
	}
}

func TestBrokerSweepSkipsIneligibleTopics(t *testing.T) {
	b, _, factory, _ := newSweepBroker(t, 10*time.Millisecond)
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() { _ = b.Shutdown(ctx) }()

	// No retention limits configured: the default config must be skipped.
	plain, err := b.CreateTopic(ctx, "plain", topic.DefaultConfig(), 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	// Compact cleanup: skipped even with limits configured.
	compacted, err := b.CreateTopic(ctx, "compacted", topic.Config{
		Cleanup:   topic.CleanupCompact,
		Retention: retention.Policy{MaxAge: time.Minute},
	}, 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	for _, name := range []topic.Name{plain.Name, compacted.Name} {
		if _, err := b.Publish(ctx, name, partition.ID(0), []message.Message{{Payload: []byte("x")}}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	b.sweep()
	for _, name := range []topic.Name{plain.Name, compacted.Name} {
		lg := b.partitionLog(t, factory, name, partition.ID(0))
		lg.mu.Lock()
		calls := lg.trimCalls
		lg.mu.Unlock()
		if calls != 0 {
			t.Fatalf("trim calls for %s = %d, want 0", name, calls)
		}
	}
}

func TestBrokerSweepReportsFailuresAndRemovals(t *testing.T) {
	b, _, factory, _ := newSweepBroker(t, 10*time.Millisecond)
	logger := &fakeLogger{}
	b.logger = logger
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() { _ = b.Shutdown(ctx) }()

	failing, err := b.CreateTopic(ctx, "failing", topic.Config{
		Cleanup:   topic.CleanupDelete,
		Retention: retention.Policy{MaxAge: time.Minute},
	}, 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	removing, err := b.CreateTopic(ctx, "removing", topic.Config{
		Cleanup:   topic.CleanupDelete,
		Retention: retention.Policy{MaxBytes: 1 << 10},
	}, 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := b.Publish(ctx, failing.Name, partition.ID(0), []message.Message{{Payload: []byte("x")}}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if _, err := b.Publish(ctx, removing.Name, partition.ID(0), []message.Message{{Payload: []byte("x")}}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	failingLog := b.partitionLog(t, factory, failing.Name, partition.ID(0))
	failingLog.mu.Lock()
	failingLog.trimErr = errors.New("disk full")
	failingLog.mu.Unlock()
	removingLog := b.partitionLog(t, factory, removing.Name, partition.ID(0))
	removingLog.mu.Lock()
	removingLog.trimResult = retention.TrimResult{Segments: 2, Bytes: 4096}
	removingLog.mu.Unlock()

	b.sweep()

	logger.mu.Lock()
	defer logger.mu.Unlock()
	if !containsMsg(logger.warns, "retention sweep failed") {
		t.Fatalf("warns logged = %v, want retention sweep failed", logger.warns)
	}
	if !containsMsg(logger.infos, "retention swept") {
		t.Fatalf("infos logged = %v, want retention swept", logger.infos)
	}
}

func containsMsg(lines []string, want string) bool {
	for _, l := range lines {
		if strings.Contains(l, want) {
			return true
		}
	}
	return false
}

func TestBrokerSweeperLifecycle(t *testing.T) {
	ctx := context.Background()

	// A disabled interval must not launch a sweeper nor block shutdown.
	disabled, _, _, _ := newSweepBroker(t, 0)
	if err := disabled.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := disabled.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	// An enabled interval starts and cleanly stops the sweeper goroutine.
	enabled, _, _, _ := newSweepBroker(t, time.Hour)
	if err := enabled.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if enabled.sweepStop == nil {
		t.Fatal("sweeper not started")
	}
	if err := enabled.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if enabled.sweepStop != nil {
		t.Fatal("sweeper not stopped after shutdown")
	}
}

func subscriptionFor(name topic.Name) consumer.Subscription {
	return consumer.Subscription{Topic: name, Partition: 0}
}
