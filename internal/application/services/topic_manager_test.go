package services

import (
	"context"
	"errors"
	"testing"

	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

func newTestTopicManager(t *testing.T) (*TopicManager, *fakeStore, *fakeFactory, *LogRegistry) {
	t.Helper()
	store := newFakeStore()
	factory := newFakeFactory()
	registry := NewLogRegistry()
	m := NewTopicManager(store, factory, registry, &fakeClock{now: timeNow()}, &fakeLogger{})
	return m, store, factory, registry
}

func TestCreateTopic(t *testing.T) {
	m, store, factory, registry := newTestTopicManager(t)
	ctx := context.Background()

	tpc, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if tpc.Name.String() != "orders" || tpc.Partitions != 1 {
		t.Fatalf("CreateTopic() = %+v, want orders with 1 partition", tpc)
	}
	if _, ok, err := store.GetTopic(ctx, tpc.Name); err != nil || !ok {
		t.Fatalf("store.GetTopic() = %v, %v; want found", ok, err)
	}
	if len(factory.created) != 1 || factory.created[0].topicName != tpc.Name || factory.created[0].partition != partition.ID(0) {
		t.Fatalf("factory.created = %v, want the partition log", factory.created)
	}
	if _, ok := registry.Log(tpc.Name, partition.ID(0)); !ok {
		t.Fatal("registry has no log for the created partition")
	}
}

func TestCreateTopicRejectsInvalidInput(t *testing.T) {
	m, store, factory, _ := newTestTopicManager(t)
	ctx := context.Background()

	cases := []struct {
		name       string
		partitions int
		want       error
	}{
		{"bad/name", 1, topic.ErrInvalidName},
		{"__reserved", 1, topic.ErrReservedName},
		{"valid", 0, topic.ErrInvalidPartitionCount},
		{"valid", 257, topic.ErrInvalidPartitionCount},
	}
	for _, c := range cases {
		if _, err := m.CreateTopic(ctx, c.name, topic.DefaultConfig(), c.partitions); !errors.Is(err, c.want) {
			t.Errorf("CreateTopic(%q, %d) error = %v, want %v", c.name, c.partitions, err, c.want)
		}
	}
	if len(store.topics) != 0 {
		t.Errorf("store has %d topics after failed creates, want 0", len(store.topics))
	}
	if len(factory.created) != 0 {
		t.Errorf("factory created %d logs after failed creates, want 0", len(factory.created))
	}
}

func TestCreateTopicAlreadyExists(t *testing.T) {
	m, _, _, _ := newTestTopicManager(t)
	ctx := context.Background()
	if _, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 1); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 1); !errors.Is(err, topic.ErrAlreadyExists) {
		t.Fatalf("CreateTopic(dup) error = %v, want ErrAlreadyExists", err)
	}
}

func TestCreateTopicRollbackOnFactoryFailure(t *testing.T) {
	m, store, factory, _ := newTestTopicManager(t)
	ctx := context.Background()
	factory.createErr = errors.New("no space")

	if _, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 1); err == nil {
		t.Fatal("CreateTopic() error = nil, want factory failure")
	}
	if _, ok, err := store.GetTopic(ctx, mustName(t, "orders")); err != nil || ok {
		t.Fatalf("store.GetTopic() after rollback = %v, %v; want not found", ok, err)
	}
	if len(factory.created) != 0 {
		t.Fatalf("factory.created after rollback = %v, want empty", factory.created)
	}
}

func TestDeleteTopic(t *testing.T) {
	m, store, factory, registry := newTestTopicManager(t)
	ctx := context.Background()

	tpc, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 1)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	lg, ok := registry.Log(tpc.Name, partition.ID(0))
	if !ok {
		t.Fatal("registry missing log before delete")
	}

	if err := m.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}
	if _, ok, _ := store.GetTopic(ctx, tpc.Name); ok {
		t.Fatal("topic still in store after delete")
	}
	if len(factory.deleted) != 1 {
		t.Fatalf("factory.deleted = %v, want the partition log", factory.deleted)
	}
	if _, ok := registry.Topic(tpc.Name); ok {
		t.Fatal("topic still registered after delete")
	}
	if fl, ok := lg.(*fakeLog); ok && !fl.closed {
		t.Fatal("log not closed during delete")
	}
}

func TestCreateTopicMultiplePartitions(t *testing.T) {
	m, store, factory, registry := newTestTopicManager(t)
	ctx := context.Background()

	tpc, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 4)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if tpc.Partitions != 4 {
		t.Fatalf("CreateTopic() Partitions = %d, want 4", tpc.Partitions)
	}
	if _, ok, err := store.GetTopic(ctx, tpc.Name); err != nil || !ok {
		t.Fatalf("store.GetTopic() = %v, %v; want found", ok, err)
	}
	if len(factory.created) != 4 {
		t.Fatalf("factory.created = %v, want 4 partition logs", factory.created)
	}
	for p := 0; p < 4; p++ {
		if _, ok := registry.Log(tpc.Name, partition.ID(p)); !ok {
			t.Fatalf("registry has no log for partition %d", p)
		}
	}
}

func TestDeleteTopicMultiplePartitions(t *testing.T) {
	m, store, factory, registry := newTestTopicManager(t)
	ctx := context.Background()

	tpc, err := m.CreateTopic(ctx, "orders", topic.DefaultConfig(), 4)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	if err := m.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}
	if _, ok, _ := store.GetTopic(ctx, tpc.Name); ok {
		t.Fatal("topic still in store after delete")
	}
	if len(factory.deleted) != 4 {
		t.Fatalf("factory.deleted = %v, want 4 partition logs", factory.deleted)
	}
	for p := 0; p < 4; p++ {
		if _, ok := registry.Log(tpc.Name, partition.ID(p)); ok {
			t.Fatalf("registry still has log for partition %d after delete", p)
		}
	}
}

func TestDeleteTopicMissing(t *testing.T) {
	m, _, _, _ := newTestTopicManager(t)
	if err := m.DeleteTopic(context.Background(), "orders"); !errors.Is(err, topic.ErrNotFound) {
		t.Fatalf("DeleteTopic() error = %v, want topic.ErrNotFound", err)
	}
}

func TestListTopicsReturnsRegistry(t *testing.T) {
	m, _, _, _ := newTestTopicManager(t)
	ctx := context.Background()
	if _, err := m.CreateTopic(ctx, "beta", topic.DefaultConfig(), 1); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := m.CreateTopic(ctx, "alpha", topic.DefaultConfig(), 1); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	topics, err := m.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics() error = %v", err)
	}
	if len(topics) != 2 || topics[0].Name.String() != "alpha" || topics[1].Name.String() != "beta" {
		t.Fatalf("ListTopics() = %v, want [alpha beta]", namesOf(topics))
	}
}

func namesOf(topics []topic.Topic) []string {
	out := make([]string, len(topics))
	for i, t := range topics {
		out[i] = t.Name.String()
	}
	return out
}
