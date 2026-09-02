package metadata

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/Yasser-Ameur/pulse/internal/domain/broker"
	"github.com/Yasser-Ameur/pulse/internal/domain/consumer"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

// InMemoryMetadataStore is a non-durable MetadataStore implementation guarded
// by a mutex. It shares the key scheme and error semantics of the Pebble store
// so that the two are interchangeable in tests and in the ephemeral CLI mode.
type InMemoryMetadataStore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// NewInMemoryMetadataStore returns an empty in-memory store.
func NewInMemoryMetadataStore() *InMemoryMetadataStore {
	return &InMemoryMetadataStore{m: make(map[string][]byte)}
}

// CreateTopic persists a new topic definition.
func (s *InMemoryMetadataStore) CreateTopic(ctx context.Context, t topic.Topic) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[string(topicKey(t.Name))]; ok {
		return topic.ErrAlreadyExists
	}
	s.m[string(topicKey(t.Name))] = mustMarshal(topicRecord{Name: t.Name, Partitions: t.Partitions, Config: t.Config, CreatedAt: t.CreatedAt})
	return nil
}

// GetTopic returns the topic definition.
func (s *InMemoryMetadataStore) GetTopic(ctx context.Context, name topic.Name) (topic.Topic, bool, error) {
	if err := ctx.Err(); err != nil {
		return topic.Topic{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.m[string(topicKey(name))]
	if !ok {
		return topic.Topic{}, false, nil
	}
	t, err := decodeTopic(data)
	if err != nil {
		return topic.Topic{}, false, err
	}
	return t, true, nil
}

// DeleteTopic removes a topic definition.
func (s *InMemoryMetadataStore) DeleteTopic(ctx context.Context, name topic.Name) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[string(topicKey(name))]; !ok {
		return topic.ErrNotFound
	}
	delete(s.m, string(topicKey(name)))
	return nil
}

// ListTopics returns all topic definitions in name order.
func (s *InMemoryMetadataStore) ListTopics(ctx context.Context) ([]topic.Topic, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.m))
	for k := range s.m {
		if len(k) >= len(prefixTopic) && k[:len(prefixTopic)] == string(prefixTopic) {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	out := make([]topic.Topic, 0, len(names))
	for _, k := range names {
		t, err := decodeTopic(s.m[k])
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// SaveCursor advances a consumer's stored position.
func (s *InMemoryMetadataStore) SaveCursor(ctx context.Context, c consumer.ID, n topic.Name, p partition.ID, o offset.Offset) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(o))
	s.m[string(cursorKey(c, n, p))] = buf[:]
	return nil
}

// GetCursor returns a consumer's stored position.
func (s *InMemoryMetadataStore) GetCursor(ctx context.Context, c consumer.ID, n topic.Name, p partition.ID) (offset.Offset, bool, error) {
	if err := ctx.Err(); err != nil {
		return offset.Invalid, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.m[string(cursorKey(c, n, p))]
	if !ok {
		return offset.Invalid, false, nil
	}
	return offset.Offset(binary.BigEndian.Uint64(data)), true, nil
}

// CreateCluster persists the cluster identity, idempotently.
func (s *InMemoryMetadataStore) CreateCluster(ctx context.Context, id broker.ClusterID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.m[string(keyClusterCurrent)]; ok {
		if string(cur) == string(id) {
			return nil
		}
		return fmt.Errorf("metadata: cluster already created")
	}
	s.m[string(clusterKey(id))] = []byte(id)
	s.m[string(keyClusterCurrent)] = []byte(id)
	return nil
}

// ClusterID returns the persisted cluster identity.
func (s *InMemoryMetadataStore) ClusterID(ctx context.Context) (broker.ClusterID, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.m[string(keyClusterCurrent)]
	if !ok {
		return "", false, nil
	}
	return broker.ClusterID(data), true, nil
}

// CreateBroker persists the broker identity, idempotently.
func (s *InMemoryMetadataStore) CreateBroker(ctx context.Context, id broker.BrokerID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.m[string(keyBrokerCurrent)]; ok {
		if string(cur) == string(id) {
			return nil
		}
		return fmt.Errorf("metadata: broker already created")
	}
	s.m[string(brokerKey(id))] = []byte(id)
	s.m[string(keyBrokerCurrent)] = []byte(id)
	return nil
}

// BrokerID returns the persisted broker identity.
func (s *InMemoryMetadataStore) BrokerID(ctx context.Context) (broker.BrokerID, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.m[string(keyBrokerCurrent)]
	if !ok {
		return "", false, nil
	}
	return broker.BrokerID(data), true, nil
}

// Close releases the store's resources. The in-memory store has none.
func (s *InMemoryMetadataStore) Close() error { return nil }

// mustMarshal serializes a record; topic records always marshal.
func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
