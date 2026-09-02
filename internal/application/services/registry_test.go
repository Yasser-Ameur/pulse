package services

import (
	"testing"

	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

func TestLogRegistryLifecycle(t *testing.T) {
	r := NewLogRegistry()
	name := mustName(t, "orders")
	pid := partition.ID(0)
	lg := newFakeLog()

	if _, ok := r.Topic(name); ok {
		t.Fatal("Topic() should not find an unregistered topic")
	}
	if _, ok := r.Log(name, pid); ok {
		t.Fatal("Log() should not find an unregistered log")
	}

	r.RegisterTopic(testTopic(name, 1))
	if got, ok := r.Topic(name); !ok || got.Name != name {
		t.Fatalf("Topic() = %v, %v; want registered", got, ok)
	}

	r.RegisterLog(name, pid, lg)
	if got, ok := r.Log(name, pid); !ok || got != lg {
		t.Fatalf("Log() = %v, %v; want registered log", got, ok)
	}
	if got := r.Logs(); len(got) != 1 || got[0] != lg {
		t.Fatalf("Logs() = %v, want the single registered log", got)
	}
	if got := r.Topics(); len(got) != 1 {
		t.Fatalf("Topics() length = %d, want 1", len(got))
	}

	dropped := r.RemoveTopic(name)
	if len(dropped) != 1 || dropped[0].partition != pid || dropped[0].log != lg {
		t.Fatalf("RemoveTopic() = %v, want the removed partition log", dropped)
	}
	if _, ok := r.Topic(name); ok {
		t.Fatal("Topic() should not find a removed topic")
	}
	if _, ok := r.Log(name, pid); ok {
		t.Fatal("Log() should not find a removed log")
	}
}

func TestLogRegistryOrdersTopics(t *testing.T) {
	r := NewLogRegistry()
	r.RegisterTopic(testTopic(mustName(t, "zeta"), 1))
	r.RegisterTopic(testTopic(mustName(t, "alpha"), 1))
	r.RegisterTopic(testTopic(mustName(t, "mid"), 1))

	got := r.Topics()
	if len(got) != 3 {
		t.Fatalf("Topics() length = %d, want 3", len(got))
	}
	want := []string{"alpha", "mid", "zeta"}
	for i, name := range want {
		if got[i].Name.String() != name {
			t.Fatalf("Topics()[%d] = %q, want %q (in name order)", i, got[i].Name, name)
		}
	}
}

// testTopic builds a minimal topic definition for registry tests.
func testTopic(name topic.Name, partitions int) topic.Topic {
	return topic.Topic{Name: name, Partitions: partitions, Config: topic.DefaultConfig()}
}
