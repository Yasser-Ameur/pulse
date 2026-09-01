package client

import "time"

// TopicConfig is the durable set of limits applied to a topic. Zero values
// fall back to the broker's own defaults.
type TopicConfig struct {
	// Partitions is the number of partitions the topic is created with.
	Partitions int
	// MaxMessageBytes bounds the payload size of a single published message.
	MaxMessageBytes int
	// Cleanup is the log cleanup policy: "delete" (default) or "compact".
	Cleanup string
	// RetentionMaxAge is the maximum age of a message before it may be
	// deleted. Zero means no time-based retention.
	RetentionMaxAge time.Duration
	// RetentionMaxBytes is the maximum size of a topic's log before the
	// oldest messages may be deleted. Zero means no size-based retention.
	RetentionMaxBytes int64
}

// Topic is a named, durably configured destination for event streams.
type Topic struct {
	Name      string
	Config    TopicConfig
	CreatedAt time.Time
}

// BrokerInfo is the broker's identity and lifecycle state.
type BrokerInfo struct {
	ClusterID string
	BrokerID  string
	NodeID    string
	Address   string
	State     string
	Version   string
}

// Message is the user-facing event model. Everything except the payload is
// advisory metadata; the broker enforces only structural validity.
type Message struct {
	// Key is an optional routing key used by future partitioning logic.
	Key string
	// Payload is the opaque event body.
	Payload []byte
	// Headers is an unordered set of user-defined attributes.
	Headers map[string]string
	// ContentType describes the payload encoding, e.g. "application/json".
	ContentType string
	// CorrelationID links events that belong to one logical operation.
	CorrelationID string
	// TraceID carries distributed-tracing context through the broker.
	TraceID string
	// EventID is the broker-assigned ULID. Empty on publish; populated on
	// records received from Subscribe.
	EventID string
}

// Record is a Message bound to its immutable log position.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Message   Message
}

// SubscribeOptions configures a Subscribe call.
type SubscribeOptions struct {
	// Consumer optionally names this consumer. When set and StartOffset is
	// nil, the stream resumes from the consumer's last acknowledged offset.
	Consumer string
	// StartOffset optionally overrides any stored cursor. Nil means "resume
	// from the consumer's cursor (or 0)".
	StartOffset *int64
	// Follow controls the end-of-log behavior. true streams new records as
	// they arrive and transparently resumes the stream on a transient
	// failure; false replays to the current end of log and returns.
	Follow bool
}
