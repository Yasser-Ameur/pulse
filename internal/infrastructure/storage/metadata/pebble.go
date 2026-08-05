package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pulse-stream/pulse/internal/domain/broker"
	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

// Schema version persisted under meta/schema-version.
const schemaVersion = 1

// Key layout (docs/Storage.md §6). topicRecord and partitionRecord are the
// JSON payloads; cluster and broker values are the id bytes themselves.
var (
	keySchemaVersion  = []byte("meta/schema-version")
	keyClusterCurrent = []byte("cluster/current")
	keyBrokerCurrent  = []byte("broker/current")
	prefixTopic       = []byte("topic/")
)

func clusterKey(id broker.ClusterID) []byte { return append([]byte("cluster/"), id...) }
func brokerKey(id broker.BrokerID) []byte   { return append([]byte("broker/"), id...) }
func topicKey(name topic.Name) []byte       { return append([]byte("topic/"), name...) }
func cursorKey(c consumer.ID, n topic.Name, p partition.ID) []byte {
	return []byte("cursor/" + string(c) + "/" + n.String() + "/" + strconv.FormatInt(int64(p), 10))
}

func partitionPrefix(name topic.Name) []byte { return append([]byte("partition/"), name...) }

// topicRecord is the serialized topic definition.
type topicRecord struct {
	Name       topic.Name
	Partitions int
	Config     topic.Config
	CreatedAt  time.Time
}

// partitionRecord is the serialized partition metadata. Phase 1 partitions are
// stateless beyond their id; replication will extend this record.
type partitionRecord struct {
	ID partition.ID
}

// PebbleMetadataStore is the production MetadataStore backed by Pebble. Every
// durable write (topic create/delete, cursor, identity) uses a sync commit
// (docs/Storage.md §6).
type PebbleMetadataStore struct {
	db *pebble.DB
}

// OpenPebble opens (creating if needed) the metadata store at path and
// verifies the persisted schema version.
func OpenPebble(path string) (*PebbleMetadataStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open metadata store: %w", err)
	}
	s := &PebbleMetadataStore{db: db}
	if err := s.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// initSchema writes the schema version on first creation and rejects a store
// written by an incompatible version.
func (s *PebbleMetadataStore) initSchema() error {
	cur, ok, err := s.get(keySchemaVersion)
	if err != nil {
		return err
	}
	if ok {
		v := int64(binary.BigEndian.Uint64(cur))
		if v != schemaVersion {
			return fmt.Errorf("metadata schema version %d, want %d", v, schemaVersion)
		}
		return nil
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(schemaVersion))
	return s.db.Set(keySchemaVersion, buf[:], pebble.Sync)
}

// CreateTopic persists a new topic definition, failing with
// topic.ErrAlreadyExists when the name is taken.
func (s *PebbleMetadataStore) CreateTopic(ctx context.Context, t topic.Topic) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok, err := s.get(topicKey(t.Name)); err != nil {
		return err
	} else if ok {
		return topic.ErrAlreadyExists
	}
	data, err := json.Marshal(topicRecord{Name: t.Name, Partitions: t.Partitions, Config: t.Config, CreatedAt: t.CreatedAt})
	if err != nil {
		return err
	}
	if err := s.db.Set(topicKey(t.Name), data, pebble.Sync); err != nil {
		return err
	}
	// Persist per-partition metadata (docs/Storage.md §6) so the schema is
	// complete for the replication phase.
	for p := 0; p < t.Partitions; p++ {
		pid := partition.ID(p)
		rec, err := json.Marshal(partitionRecord{ID: pid})
		if err != nil {
			return err
		}
		key := append(partitionPrefix(t.Name), []byte("/"+strconv.FormatInt(int64(p), 10))...)
		if err := s.db.Set(key, rec, pebble.Sync); err != nil {
			return err
		}
	}
	return nil
}

// GetTopic returns the topic definition.
func (s *PebbleMetadataStore) GetTopic(ctx context.Context, name topic.Name) (topic.Topic, bool, error) {
	if err := ctx.Err(); err != nil {
		return topic.Topic{}, false, err
	}
	data, ok, err := s.get(topicKey(name))
	if err != nil || !ok {
		return topic.Topic{}, ok, err
	}
	t, err := decodeTopic(data)
	if err != nil {
		return topic.Topic{}, false, err
	}
	return t, true, nil
}

// DeleteTopic removes the topic and its partition metadata.
func (s *PebbleMetadataStore) DeleteTopic(ctx context.Context, name topic.Name) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok, err := s.get(topicKey(name)); err != nil {
		return err
	} else if !ok {
		return topic.ErrNotFound
	}
	if err := s.db.Delete(topicKey(name), pebble.Sync); err != nil {
		return err
	}
	return s.deletePrefix(partitionPrefix(name))
}

// ListTopics returns all topic definitions in name order.
func (s *PebbleMetadataStore) ListTopics(ctx context.Context) ([]topic.Topic, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keys, err := s.prefixKeys(prefixTopic)
	if err != nil {
		return nil, err
	}
	out := make([]topic.Topic, 0, len(keys))
	for _, k := range keys {
		data, ok, err := s.get(k)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		t, err := decodeTopic(data)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// SaveCursor advances a consumer's stored position.
func (s *PebbleMetadataStore) SaveCursor(ctx context.Context, c consumer.ID, n topic.Name, p partition.ID, o offset.Offset) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(o))
	return s.db.Set(cursorKey(c, n, p), buf[:], pebble.Sync)
}

// GetCursor returns a consumer's stored position.
func (s *PebbleMetadataStore) GetCursor(ctx context.Context, c consumer.ID, n topic.Name, p partition.ID) (offset.Offset, bool, error) {
	if err := ctx.Err(); err != nil {
		return offset.Invalid, false, err
	}
	data, ok, err := s.get(cursorKey(c, n, p))
	if err != nil || !ok {
		return offset.Invalid, ok, err
	}
	return offset.Offset(binary.BigEndian.Uint64(data)), true, nil
}

// CreateCluster persists the cluster identity. It is idempotent for the same
// id and rejects a second, different cluster in the same store.
func (s *PebbleMetadataStore) CreateCluster(ctx context.Context, id broker.ClusterID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cur, ok, err := s.get(keyClusterCurrent); err != nil {
		return err
	} else if ok {
		if string(cur) == string(id) {
			return nil
		}
		return fmt.Errorf("metadata: cluster already created")
	}
	if err := s.db.Set(clusterKey(id), []byte(id), pebble.Sync); err != nil {
		return err
	}
	return s.db.Set(keyClusterCurrent, []byte(id), pebble.Sync)
}

// ClusterID returns the persisted cluster identity.
func (s *PebbleMetadataStore) ClusterID(ctx context.Context) (broker.ClusterID, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	data, ok, err := s.get(keyClusterCurrent)
	if err != nil || !ok {
		return "", ok, err
	}
	return broker.ClusterID(data), true, nil
}

// CreateBroker persists the broker identity. It is idempotent for the same id
// and rejects a second, different broker in the same store.
func (s *PebbleMetadataStore) CreateBroker(ctx context.Context, id broker.BrokerID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cur, ok, err := s.get(keyBrokerCurrent); err != nil {
		return err
	} else if ok {
		if string(cur) == string(id) {
			return nil
		}
		return fmt.Errorf("metadata: broker already created")
	}
	if err := s.db.Set(brokerKey(id), []byte(id), pebble.Sync); err != nil {
		return err
	}
	return s.db.Set(keyBrokerCurrent, []byte(id), pebble.Sync)
}

// BrokerID returns the persisted broker identity.
func (s *PebbleMetadataStore) BrokerID(ctx context.Context) (broker.BrokerID, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	data, ok, err := s.get(keyBrokerCurrent)
	if err != nil || !ok {
		return "", ok, err
	}
	return broker.BrokerID(data), true, nil
}

// Close flushes and closes the underlying Pebble database.
func (s *PebbleMetadataStore) Close() error {
	return s.db.Close()
}

// get reads a key, returning a copy of its value and whether it exists.
func (s *PebbleMetadataStore) get(key []byte) ([]byte, bool, error) {
	v, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	data := bytes.Clone(v)
	_ = closer.Close()
	return data, true, nil
}

// prefixKeys returns the keys at or after prefix in sorted order, with an
// exclusive upper bound at the end of the prefix.
func (s *PebbleMetadataStore) prefixKeys(prefix []byte) ([][]byte, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: incrementUpperBound(prefix)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, bytes.Clone(iter.Key()))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}

// deletePrefix removes every key at or after prefix.
func (s *PebbleMetadataStore) deletePrefix(prefix []byte) error {
	keys, err := s.prefixKeys(prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := s.db.Delete(k, pebble.Sync); err != nil {
			return err
		}
	}
	return nil
}

// incrementUpperBound computes the smallest byte slice greater than every key
// starting with prefix, for use as an exclusive range upper bound.
func incrementUpperBound(prefix []byte) []byte {
	out := bytes.Clone(prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return append(out, 0)
}

// decodeTopic deserializes a stored topic definition.
func decodeTopic(data []byte) (topic.Topic, error) {
	var rec topicRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return topic.Topic{}, fmt.Errorf("decode topic: %w", err)
	}
	return topic.Topic{Name: rec.Name, Partitions: rec.Partitions, Config: rec.Config, CreatedAt: rec.CreatedAt}, nil
}
