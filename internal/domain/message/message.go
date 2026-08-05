// Package message defines the event model: the user-facing Message, the
// persisted Record, and the ordered RecordBatch, together with their
// validation rules.
//
// Event identifiers are ULIDs (see EventID). The package owns no I/O and no
// transport types; it is the purest expression of what an event is.
package message

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/pulse-stream/pulse/internal/domain/offset"
)

// Structural limits enforced for every message regardless of topic
// configuration. Topic-specific limits (payload size) are applied by the
// application layer using the topic configuration.
const (
	// MaxHeaders is the maximum number of headers a message may carry.
	MaxHeaders = 64
	// MaxHeaderKeyBytes is the maximum length of a single header key.
	MaxHeaderKeyBytes = 512
	// MaxHeaderValueBytes is the maximum length of a single header value.
	MaxHeaderValueBytes = 8192
	// MaxKeyBytes is the maximum length of a message key.
	MaxKeyBytes = 4096
	// SystemHeaderPrefix reserves the header key namespace used to persist
	// message metadata on disk (event id, content type, tracing fields, ...).
	// Producers may not use it; the codec maps these keys to Message fields
	// transparently.
	SystemHeaderPrefix = "__pulse."
)

// Sentinel errors returned by message validation.
var (
	// ErrInvalidMessage is the root of all structural message errors.
	ErrInvalidMessage = errors.New("invalid message")
	// ErrHeadersTooMany reports that a message exceeded MaxHeaders.
	ErrHeadersTooMany = fmt.Errorf("%w: too many headers", ErrInvalidMessage)
	// ErrHeaderKeyTooLong reports an oversized header key.
	ErrHeaderKeyTooLong = fmt.Errorf("%w: header key too long", ErrInvalidMessage)
	// ErrHeaderValueTooLong reports an oversized header value.
	ErrHeaderValueTooLong = fmt.Errorf("%w: header value too long", ErrInvalidMessage)
	// ErrHeaderKeyReserved reports a header key in the reserved system
	// namespace.
	ErrHeaderKeyReserved = fmt.Errorf("%w: reserved header key", ErrInvalidMessage)
	// ErrKeyTooLong reports an oversized message key.
	ErrKeyTooLong = fmt.Errorf("%w: key too long", ErrInvalidMessage)
	// ErrPayloadTooLarge reports a payload exceeding the topic limit.
	ErrPayloadTooLarge = fmt.Errorf("%w: payload too large", ErrInvalidMessage)
	// ErrInvalidEventID reports a malformed event identifier.
	ErrInvalidEventID = fmt.Errorf("%w: invalid event id", ErrInvalidMessage)
	// ErrEmptyBatch reports an attempt to persist a batch without records.
	ErrEmptyBatch = fmt.Errorf("%w: empty batch", ErrInvalidMessage)
)

// EventID is a ULID identifying a single event. ULIDs are lexicographically
// sortable and timestamp-ordered, which makes them well suited to operational
// debugging and index locality.
type EventID ulid.ULID

// Zero reports whether the event ID is the zero value, meaning "unassigned".
func (id EventID) Zero() bool { return id == EventID{} }

// String returns the canonical 26-character Crockford base32 representation.
func (id EventID) String() string { return ulid.ULID(id).String() }

// NewEventID generates a new event ID carrying the given timestamp.
func NewEventID(now time.Time) EventID {
	return EventID(ulid.MustNewDefault(now))
}

// ParseEventID parses s as a ULID, tolerating lowercase input.
func ParseEventID(s string) (EventID, error) {
	u, err := ulid.Parse(strings.ToUpper(s))
	if err != nil {
		return EventID{}, ErrInvalidEventID
	}
	return EventID(u), nil
}

// Headers is an unordered set of user-defined attributes attached to an event.
type Headers map[string]string

// Message is the user-facing event model. Everything except the payload is
// advisory metadata; the broker enforces only structural validity.
type Message struct {
	// EventID is zero when the broker is expected to assign one at publish
	// time.
	EventID EventID
	// Key is an optional routing key used by future partitioning logic.
	Key string
	// Payload is the opaque event body.
	Payload []byte
	// Headers is an unordered set of user-defined attributes.
	Headers Headers
	// ContentType describes the payload encoding, e.g. "application/json".
	ContentType string
	// CorrelationID links events that belong to one logical operation.
	CorrelationID string
	// TraceID carries distributed-tracing context through the broker.
	TraceID string
	// RetryCount records how many times the event has been redelivered.
	RetryCount int32
	// TTL is the event time-to-live; zero means no TTL.
	TTL time.Duration
	// Priority orders messages within a partition; lower is higher.
	Priority int32
	// SchemaVersion identifies the payload schema; zero means unversioned.
	SchemaVersion int32
}

// Validate checks the structural invariants of the message. maxPayloadBytes is
// the topic's configured maximum payload size.
func (m Message) Validate(maxPayloadBytes int64) error {
	if int64(len(m.Payload)) > maxPayloadBytes {
		return ErrPayloadTooLarge
	}
	if len(m.Key) > MaxKeyBytes {
		return ErrKeyTooLong
	}
	if len(m.Headers) > MaxHeaders {
		return ErrHeadersTooMany
	}
	for k, v := range m.Headers {
		if strings.HasPrefix(k, SystemHeaderPrefix) {
			return ErrHeaderKeyReserved
		}
		if len(k) > MaxHeaderKeyBytes {
			return ErrHeaderKeyTooLong
		}
		if len(v) > MaxHeaderValueBytes {
			return ErrHeaderValueTooLong
		}
	}
	return nil
}

// Record is a Message bound to its immutable log position. Records are what
// consumers receive; the topic and partition are implied by the stream they
// are read from.
type Record struct {
	// Offset is the record's position within its partition log.
	Offset offset.Offset
	// Timestamp is the broker-assigned wall-clock time of the record.
	Timestamp time.Time
	// Message is the event body with all metadata.
	Message Message
}

// RecordBatch is an ordered group of records persisted atomically. All records
// in a batch receive consecutive offsets. The codec serializes batches; the
// log engine assigns offsets at append time.
type RecordBatch struct {
	// BaseOffset is the first offset of the batch, assigned by the log on
	// append.
	BaseOffset offset.Offset
	// FirstTimestamp is the earliest record timestamp in the batch.
	FirstTimestamp time.Time
	// LastTimestamp is the latest record timestamp in the batch.
	LastTimestamp time.Time
	// Records holds the batch contents in order.
	Records []Record
}
