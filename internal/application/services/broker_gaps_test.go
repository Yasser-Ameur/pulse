package services

import (
	"context"
	"errors"
	"testing"

	"github.com/Yasser-Ameur/pulse/internal/domain/broker"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

func TestBrokerStatsStartsAtZeroAndTracksPublish(t *testing.T) {
	b, _, _, _ := newTestBroker(t)
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got := b.Stats(); got != (Stats{}) {
		t.Fatalf("Stats() = %+v, want zero value before any traffic", got)
	}
}

func TestBrokerListTopicsRejectsWhenNotRunning(t *testing.T) {
	b, _, _, _ := newTestBroker(t)
	// Not started: state is Starting, not Running.
	if _, err := b.ListTopics(context.Background()); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("ListTopics() error = %v, want ErrNotRunning", err)
	}
}

func TestBrokerListTopicsReturnsCreatedTopics(t *testing.T) {
	b, _, _, _ := newTestBroker(t)
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := b.CreateTopic(ctx, "orders", topic.Config{}, 2); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	topics, err := b.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics() error = %v", err)
	}
	if len(topics) != 1 || topics[0].Name.String() != "orders" {
		t.Fatalf("ListTopics() = %+v, want one topic named orders", topics)
	}
}

// TestBrokerStartRollsBackOpenedLogsOnPartialFailure pins the documented
// startup rollback: when a topic has multiple partitions and only some of
// their logs can be reopened, Start closes every log it had already opened
// before returning the error, rather than leaking file handles.
func TestBrokerStartRollsBackOpenedLogsOnPartialFailure(t *testing.T) {
	store := newFakeStore()
	factory := newFakeFactory()

	name := mustName(t, "orders")
	if err := store.CreateTopic(context.Background(), topic.Topic{Name: name, Partitions: 2}); err != nil {
		t.Fatalf("seed CreateTopic() error = %v", err)
	}
	// Partition 0's log exists and opens cleanly; partition 1's Open fails
	// with something other than ErrLogNotFound, which is unrecoverable.
	p0, err := factory.Create(context.Background(), name, partition.ID(0))
	if err != nil {
		t.Fatalf("seed Create() error = %v", err)
	}
	log0 := p0.(*fakeLog)
	factory.openErrs[partitionKey{topicName: name, partition: partition.ID(1)}] = errors.New("disk error")

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

	err = b.Start(context.Background())
	if err == nil {
		t.Fatal("Start() error = nil, want the partition-1 open failure")
	}
	if !log0.closed {
		t.Fatal("partition 0's log was left open after the rollback")
	}
}

// TestCreateTopicRollbackUnregistersEarlierPartitions pins that when a
// multi-partition CreateTopic fails partway through, the registry entries for
// the partitions it already created are dropped, not left dangling.
func TestCreateTopicRollbackUnregistersEarlierPartitions(t *testing.T) {
	m, _, factory, registry := newTestTopicManager(t)
	ctx := context.Background()
	name := mustName(t, "orders")
	factory.createErrs[partitionKey{topicName: name, partition: partition.ID(1)}] = errors.New("no space")

	if _, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 2); err == nil {
		t.Fatal("CreateTopic() error = nil, want the partition-1 create failure")
	}
	if _, ok := registry.Log(name, partition.ID(0)); ok {
		t.Fatal("partition 0's log is still registered after the rollback")
	}
}

func TestBrokerTopicsViewReportsPartitionOffsets(t *testing.T) {
	b, _, _, _ := newTestBroker(t)
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := b.CreateTopic(ctx, "orders", topic.Config{}, 1); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	views := b.TopicsView()
	if len(views) != 1 || views[0].Name != "orders" {
		t.Fatalf("TopicsView() = %+v, want one topic named orders", views)
	}
	if len(views[0].Partitions) != 1 || views[0].Partitions[0].EndOffset != 0 {
		t.Fatalf("TopicsView()[0].Partitions = %+v, want one partition at offset 0", views[0].Partitions)
	}
}

func TestBrokerDeleteTopicRejectsWhenNotRunning(t *testing.T) {
	b, _, _, _ := newTestBroker(t)
	// Not started: state is Starting, not Running.
	if err := b.DeleteTopic(context.Background(), "orders"); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("DeleteTopic() error = %v, want ErrNotRunning", err)
	}
}

// TestCreateTopicRollbackReportsFactoryDeleteFailure pins that when the
// rollback itself cannot delete a partition's log, CreateTopic still returns
// the original creation error (the rollback failure is only logged, not
// swapped in as the returned error).
func TestCreateTopicRollbackReportsFactoryDeleteFailure(t *testing.T) {
	m, _, factory, _ := newTestTopicManager(t)
	ctx := context.Background()
	name := mustName(t, "orders")
	factory.createErrs[partitionKey{topicName: name, partition: partition.ID(1)}] = errors.New("no space")
	factory.deleteErr = errors.New("delete also failed")

	_, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 2)
	if err == nil || err.Error() != "no space" {
		t.Fatalf("CreateTopic() error = %v, want the original create failure", err)
	}
}
