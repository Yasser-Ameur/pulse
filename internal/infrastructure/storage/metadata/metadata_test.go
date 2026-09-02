package metadata

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/application/ports"
	"github.com/Yasser-Ameur/pulse/internal/domain/broker"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/retention"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
	"github.com/cockroachdb/pebble"
)

func testTopic(name string, partitions int) topic.Topic {
	n, _ := topic.NewName(name)
	return topic.Topic{
		Name:       n,
		Partitions: partitions,
		Config: topic.Config{
			MaxMessageBytes:   1024,
			Retention:         retention.Policy{MaxAge: time.Hour, MaxBytes: 1 << 20},
			Cleanup:           topic.CleanupDelete,
			ReplicationFactor: 1,
		},
		CreatedAt: time.Unix(1700000000, 0).UTC(),
	}
}

func TestMetadataStore(t *testing.T) {
	stores := map[string]func(t *testing.T) ports.MetadataStore{
		"pebble": func(t *testing.T) ports.MetadataStore {
			s, err := OpenPebble(t.TempDir())
			if err != nil {
				t.Fatalf("OpenPebble() error = %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			return s
		},
		"inmemory": func(t *testing.T) ports.MetadataStore {
			return NewInMemoryMetadataStore()
		},
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) { runMetadataTests(t, open(t)) })
	}
}

func runMetadataTests(t *testing.T, s ports.MetadataStore) {
	ctx := context.Background()

	t.Run("topics", func(t *testing.T) {
		orders := testTopic("orders", 1)
		if err := s.CreateTopic(ctx, orders); err != nil {
			t.Fatalf("CreateTopic() error = %v", err)
		}
		if err := s.CreateTopic(ctx, orders); !errors.Is(err, topic.ErrAlreadyExists) {
			t.Fatalf("CreateTopic(dup) error = %v, want ErrAlreadyExists", err)
		}

		got, ok, err := s.GetTopic(ctx, orders.Name)
		if err != nil || !ok {
			t.Fatalf("GetTopic() = (%v, %v, %v), want ok", got, ok, err)
		}
		if got.Name != orders.Name || got.Partitions != 1 ||
			got.Config.MaxMessageBytes != 1024 || got.Config.Retention.MaxAge != time.Hour {
			t.Fatalf("GetTopic() round-trip = %+v, want %+v", got, orders)
		}
		if got.CreatedAt.Unix() != orders.CreatedAt.Unix() {
			t.Fatalf("CreatedAt = %v, want %v", got.CreatedAt, orders.CreatedAt)
		}

		if err := s.CreateTopic(ctx, testTopic("alerts", 1)); err != nil {
			t.Fatalf("CreateTopic(alerts) error = %v", err)
		}
		all, err := s.ListTopics(ctx)
		if err != nil {
			t.Fatalf("ListTopics() error = %v", err)
		}
		if len(all) != 2 || all[0].Name.String() != "alerts" || all[1].Name.String() != "orders" {
			t.Fatalf("ListTopics() = %v, want [alerts orders]", all)
		}

		if err := s.DeleteTopic(ctx, orders.Name); err != nil {
			t.Fatalf("DeleteTopic() error = %v", err)
		}
		if _, ok, _ := s.GetTopic(ctx, orders.Name); ok {
			t.Fatal("GetTopic() after delete found, want not found")
		}
		if err := s.DeleteTopic(ctx, orders.Name); !errors.Is(err, topic.ErrNotFound) {
			t.Fatalf("DeleteTopic(missing) error = %v, want ErrNotFound", err)
		}
	})

	t.Run("cursor", func(t *testing.T) {
		if _, ok, _ := s.GetCursor(ctx, "reader", testTopic("orders", 1).Name, partition.ID(0)); ok {
			t.Fatal("GetCursor() on fresh consumer found, want not found")
		}
		n, _ := topic.NewName("orders")
		if err := s.SaveCursor(ctx, "reader", n, partition.ID(0), offset.Offset(7)); err != nil {
			t.Fatalf("SaveCursor() error = %v", err)
		}
		got, ok, err := s.GetCursor(ctx, "reader", n, partition.ID(0))
		if err != nil || !ok || got != 7 {
			t.Fatalf("GetCursor() = (%v, %v, %v), want (7, true, nil)", got, ok, err)
		}
		if err := s.SaveCursor(ctx, "reader", n, partition.ID(0), offset.Offset(42)); err != nil {
			t.Fatalf("SaveCursor() error = %v", err)
		}
		if got, _, _ := s.GetCursor(ctx, "reader", n, partition.ID(0)); got != 42 {
			t.Fatalf("GetCursor() = %v, want 42 after advance", got)
		}
	})

	t.Run("identity", func(t *testing.T) {
		cid := broker.ClusterID("cluster-A")
		if err := s.CreateCluster(ctx, cid); err != nil {
			t.Fatalf("CreateCluster() error = %v", err)
		}
		if err := s.CreateCluster(ctx, cid); err != nil {
			t.Fatalf("CreateCluster(idempotent) error = %v", err)
		}
		got, ok, err := s.ClusterID(ctx)
		if err != nil || !ok || got != cid {
			t.Fatalf("ClusterID() = (%v, %v, %v), want (%v, true, nil)", got, ok, err, cid)
		}
		if err := s.CreateCluster(ctx, broker.ClusterID("cluster-B")); err == nil {
			t.Fatal("CreateCluster(conflict) error = nil, want error")
		}

		bid := broker.BrokerID("broker-1")
		if err := s.CreateBroker(ctx, bid); err != nil {
			t.Fatalf("CreateBroker() error = %v", err)
		}
		if err := s.CreateBroker(ctx, bid); err != nil {
			t.Fatalf("CreateBroker(idempotent) error = %v", err)
		}
		gotB, ok, err := s.BrokerID(ctx)
		if err != nil || !ok || gotB != bid {
			t.Fatalf("BrokerID() = (%v, %v, %v), want (%v, true, nil)", gotB, ok, err, bid)
		}
		if err := s.CreateBroker(ctx, broker.BrokerID("broker-2")); err == nil {
			t.Fatal("CreateBroker(conflict) error = nil, want error")
		}
	})
}

func TestPebbleSchemaVersionConflict(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble() error = %v", err)
	}
	// Corrupt the schema version so a reopen rejects the store.
	if err := s.db.Set(keySchemaVersion, []byte{0, 0, 0, 0, 0, 0, 0, 99}, pebble.Sync); err != nil {
		t.Fatalf("corrupt schema: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := OpenPebble(dir); err == nil {
		t.Fatal("OpenPebble() with wrong schema error = nil, want error")
	}
}

func TestPebbleReopenDurability(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble() error = %v", err)
	}
	orders := testTopic("orders", 1)
	if err := s.CreateTopic(context.Background(), orders); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble() error = %v", err)
	}
	defer func() { _ = reopened.Close() }()
	got, ok, err := reopened.GetTopic(context.Background(), orders.Name)
	if err != nil || !ok || got.Name != orders.Name {
		t.Fatalf("GetTopic() after reopen = (%v, %v, %v)", got, ok, err)
	}
}
