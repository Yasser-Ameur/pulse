// Package services contains the application orchestration: the Broker facade,
// topic management, publishing, and subscribing. These types encode the
// business rules (offset assignment, cursor semantics, lifecycle transitions)
// and depend only on the domain and the ports package.
package services

import (
	"sync"

	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/storage"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

// Structured logging field keys shared across services.
const (
	logKeyTopic     = "topic"
	logKeyPartition = "partition"
	logKeyError     = "error"
)

// partitionKey uniquely identifies a partition across all topics.
type partitionKey struct {
	topicName topic.Name
	partition partition.ID
}

// LogRegistry is the in-memory view of the broker's topics and their open
// logs. It is the single owner of the topic-to-log lifecycle and is safe for
// concurrent use.
//
// The registry deliberately does not open, close, or delete logs itself; it
// records decisions made by the Broker facade so that those operations can be
// ordered correctly around recovery and shutdown.
type LogRegistry struct {
	mu     sync.RWMutex
	topics map[topic.Name]topic.Topic
	logs   map[partitionKey]storage.Log
}

// NewLogRegistry returns an empty registry.
func NewLogRegistry() *LogRegistry {
	return &LogRegistry{
		topics: make(map[topic.Name]topic.Topic),
		logs:   make(map[partitionKey]storage.Log),
	}
}

// RegisterTopic records a topic definition. An existing entry is replaced.
func (r *LogRegistry) RegisterTopic(t topic.Topic) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics[t.Name] = t
}

// partitionLog pairs a partition id with its open log.
type partitionLog struct {
	partition partition.ID
	log       storage.Log
}

// RemoveTopic drops the topic definition and every log reference for it,
// returning the removed logs so the caller can close and delete them.
func (r *LogRegistry) RemoveTopic(name topic.Name) []partitionLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.topics, name)
	var dropped []partitionLog
	for k, lg := range r.logs {
		if k.topicName == name {
			delete(r.logs, k)
			dropped = append(dropped, partitionLog{partition: k.partition, log: lg})
		}
	}
	return dropped
}

// Topic returns the topic definition and whether it exists.
func (r *LogRegistry) Topic(name topic.Name) (topic.Topic, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.topics[name]
	return t, ok
}

// Topics returns all registered topic definitions in name order.
func (r *LogRegistry) Topics() []topic.Topic {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]topic.Topic, 0, len(r.topics))
	for _, t := range r.topics {
		out = append(out, t)
	}
	sortTopics(out)
	return out
}

// RegisterLog records the open log for a partition.
func (r *LogRegistry) RegisterLog(name topic.Name, id partition.ID, lg storage.Log) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs[partitionKey{topicName: name, partition: id}] = lg
}

// UnregisterLog drops the log reference for a partition. The caller closes and
// deletes the log itself.
func (r *LogRegistry) UnregisterLog(name topic.Name, id partition.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.logs, partitionKey{topicName: name, partition: id})
}

// Log returns the open log for a partition and whether it exists.
func (r *LogRegistry) Log(name topic.Name, id partition.ID) (storage.Log, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	lg, ok := r.logs[partitionKey{topicName: name, partition: id}]
	return lg, ok
}

// Logs returns every open log. Used by shutdown to sync and close all logs.
func (r *LogRegistry) Logs() []storage.Log {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]storage.Log, 0, len(r.logs))
	for _, lg := range r.logs {
		out = append(out, lg)
	}
	return out
}

// sortTopics orders topics by name for deterministic output.
func sortTopics(topics []topic.Topic) {
	for i := 1; i < len(topics); i++ {
		for j := i; j > 0 && topics[j].Name < topics[j-1].Name; j-- {
			topics[j], topics[j-1] = topics[j-1], topics[j]
		}
	}
}
