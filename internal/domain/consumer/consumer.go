// Package consumer defines consumer identity and subscriptions.
//
// Consumers are identified by a name so their position survives reconnects.
// Consumer groups (multiple members sharing a workload) build on the same
// identity model in a later phase.
package consumer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

// Limits applied to consumer names.
const (
	// MaxNameLength is the maximum length of a consumer name.
	MaxNameLength = 128
)

// Sentinel errors returned by consumer operations.
var (
	// ErrInvalidName reports an invalid consumer name.
	ErrInvalidName = errors.New("invalid consumer name")
	// ErrInvalidSubscription reports an invalid subscription.
	ErrInvalidSubscription = fmt.Errorf("%w: invalid subscription", ErrInvalidName)
)

// ID is the name of a consumer. It must be unique per partition position.
type ID string

// String returns the consumer name.
func (id ID) String() string { return string(id) }

// Validate checks the consumer name. Names must be non-empty, bounded, and
// free of characters that would clash with metadata keys.
func (id ID) Validate() error {
	s := string(id)
	if s == "" || len(s) > MaxNameLength {
		return ErrInvalidName
	}
	if strings.ContainsAny(s, "/\x00") {
		return ErrInvalidName
	}
	return nil
}

// Subscription captures a consumer's intent to read a topic.
type Subscription struct {
	// Consumer is the consumer name. Empty means no cursor tracking.
	Consumer ID
	// Topic is the topic to read.
	Topic topic.Name
	// Partition is the partition to read.
	Partition partition.ID
	// Start is the explicit start offset. A nil value means "resume from the
	// consumer's stored cursor (or 0 if never set)".
	Start *offset.Offset
	// Follow controls end-of-log behavior: true streams new records, false
	// replays to the current end and completes.
	Follow bool
}

// Validate checks the subscription for structural validity.
func (s Subscription) Validate() error {
	if s.Consumer != "" {
		if err := s.Consumer.Validate(); err != nil {
			return err
		}
	}
	if s.Topic == "" {
		return fmt.Errorf("%w: empty topic", ErrInvalidSubscription)
	}
	return nil
}
