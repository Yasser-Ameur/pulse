// Package topic defines the topic aggregate and its value objects.
//
// A topic is a named, durably configured destination for event streams. The
// package owns name syntax, configuration limits, and the errors the rest of
// the system relies on (not found, already exists).
package topic

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/retention"
)

// Limits applied to topic names and configuration.
const (
	// MaxNameLength is the maximum length of a topic name.
	MaxNameLength = 255
	// DefaultMaxMessageBytes is the default per-message payload limit.
	DefaultMaxMessageBytes int64 = 1 << 20 // 1 MiB
	// MaxMessageBytes is the hard cap on a topic's per-message payload limit.
	MaxMessageBytes int64 = 1 << 26 // 64 MiB
	// MaxPartitions is the hard cap on a topic's partition count.
	MaxPartitions = 256
)

// namePattern restricts topic names to URL- and path-safe characters. Names
// may not contain path separators because they are used as filesystem paths.
var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// Sentinel errors returned by topic operations.
var (
	// ErrInvalidName reports a topic name that violates the naming rules.
	ErrInvalidName = errors.New("invalid topic name")
	// ErrReservedName reports a name in the reserved internal namespace.
	ErrReservedName = errors.New("reserved topic name")
	// ErrInvalidConfig reports an invalid topic configuration.
	ErrInvalidConfig = errors.New("invalid topic config")
	// ErrNotFound reports that a topic does not exist.
	ErrNotFound = errors.New("topic not found")
	// ErrAlreadyExists reports that a topic already exists.
	ErrAlreadyExists = errors.New("topic already exists")
	// ErrInvalidPartitionCount reports an out-of-range partition count.
	ErrInvalidPartitionCount = fmt.Errorf("%w: invalid partition count", ErrInvalidConfig)
)

// Name is a validated topic name.
type Name string

// NewName validates and constructs a topic name. Empty, oversized, path-like,
// and reserved names are rejected.
func NewName(s string) (Name, error) {
	if s == "" || len(s) > MaxNameLength {
		return "", ErrInvalidName
	}
	if s == "." || s == ".." {
		return "", ErrInvalidName
	}
	if strings.HasPrefix(s, "__") {
		return "", ErrReservedName
	}
	if !namePattern.MatchString(s) {
		return "", ErrInvalidName
	}
	return Name(s), nil
}

// String returns the topic name as a string.
func (n Name) String() string { return string(n) }

// CleanupPolicy determines how a topic's log is trimmed.
type CleanupPolicy string

const (
	// CleanupDelete removes whole segments once they fall outside retention.
	CleanupDelete CleanupPolicy = "delete"
	// CleanupCompact rewrites the log keeping only the last record per key.
	// Reserved for a future phase.
	CleanupCompact CleanupPolicy = "compact"
)

// Config is the durable set of limits applied to a topic.
type Config struct {
	// MaxMessageBytes bounds the payload size of a single published message.
	MaxMessageBytes int64
	// Retention holds the time and size limits for the topic log.
	Retention retention.Policy
	// Cleanup is the log cleanup policy.
	Cleanup CleanupPolicy
	// ReplicationFactor is the number of replicas per partition. Phase 1
	// brokers are single-node and always use one.
	ReplicationFactor int32
}

// DefaultConfig returns the broker defaults for a topic.
func DefaultConfig() Config {
	return Config{
		MaxMessageBytes:   DefaultMaxMessageBytes,
		Retention:         retention.Policy{},
		Cleanup:           CleanupDelete,
		ReplicationFactor: 1,
	}
}

// Validate checks the configuration against its limits. Zero values fall back
// to the broker defaults.
func (c Config) Validate() (Config, error) {
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if c.MaxMessageBytes < 1 || c.MaxMessageBytes > MaxMessageBytes {
		return Config{}, fmt.Errorf("%w: max message bytes %d", ErrInvalidConfig, c.MaxMessageBytes)
	}
	if err := c.Retention.Validate(); err != nil {
		return Config{}, err
	}
	switch c.Cleanup {
	case "", CleanupDelete:
		c.Cleanup = CleanupDelete
	case CleanupCompact:
		// Compaction is not enforced in Phase 1 but is accepted so that the
		// configuration round-trips.
	default:
		return Config{}, fmt.Errorf("%w: unknown cleanup policy %q", ErrInvalidConfig, c.Cleanup)
	}
	if c.ReplicationFactor == 0 {
		c.ReplicationFactor = 1
	}
	if c.ReplicationFactor != 1 {
		return Config{}, fmt.Errorf("%w: replication factor %d not supported in phase 1", ErrInvalidConfig, c.ReplicationFactor)
	}
	return c, nil
}

// Topic is the aggregate root of a named event stream.
type Topic struct {
	// Name uniquely identifies the topic within the cluster.
	Name Name
	// Partitions is the number of partitions, from 1 to MaxPartitions.
	Partitions int
	// Config holds the durable topic limits.
	Config Config
	// CreatedAt is the broker-assigned creation time.
	CreatedAt time.Time
}
