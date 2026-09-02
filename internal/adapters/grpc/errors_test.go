package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/broker"
	"github.com/Yasser-Ameur/pulse/internal/domain/consumer"
	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/retention"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
	"github.com/Yasser-Ameur/pulse/pkg/api/pulse/v1/pulsepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapErrorNil(t *testing.T) {
	if err := mapError(nil); err != nil {
		t.Errorf("mapError(nil) = %v, want nil", err)
	}
}

func TestMapError(t *testing.T) {
	tests := []struct {
		name string
		in   error
		want codes.Code
	}{
		{name: "context canceled", in: context.Canceled, want: codes.Canceled},
		{name: "deadline exceeded", in: context.DeadlineExceeded, want: codes.DeadlineExceeded},
		{name: "invalid topic name", in: topic.ErrInvalidName, want: codes.InvalidArgument},
		{name: "invalid topic config", in: topic.ErrInvalidConfig, want: codes.InvalidArgument},
		{name: "invalid partition count", in: topic.ErrInvalidPartitionCount, want: codes.InvalidArgument},
		{name: "reserved topic name", in: topic.ErrReservedName, want: codes.InvalidArgument},
		{name: "invalid message", in: message.ErrInvalidMessage, want: codes.InvalidArgument},
		{name: "batch too large", in: message.ErrBatchTooLarge, want: codes.InvalidArgument},
		{name: "invalid consumer name", in: consumer.ErrInvalidName, want: codes.InvalidArgument},
		{name: "topic not found", in: topic.ErrNotFound, want: codes.NotFound},
		{name: "partition not found", in: partition.ErrNotFound, want: codes.NotFound},
		{name: "topic exists", in: topic.ErrAlreadyExists, want: codes.AlreadyExists},
		{name: "offset out of range", in: offset.ErrOutOfRange, want: codes.OutOfRange},
		{name: "offset invalid wraps out of range", in: offset.ErrInvalid, want: codes.OutOfRange},
		{name: "broker not running", in: broker.ErrNotRunning, want: codes.Unavailable},
		{name: "broker draining", in: broker.ErrDraining, want: codes.Unavailable},
		{name: "unrecognized", in: errors.New("some internal failure"), want: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapError(tt.in)
			if got == nil {
				t.Fatalf("mapError(%v) = nil, want code %v", tt.in, tt.want)
			}
			if code := status.Code(got); code != tt.want {
				t.Errorf("mapError(%v) code = %v, want %v", tt.in, code, tt.want)
			}
		})
	}
}

// TestMapErrorUnwrapsWrappedDomainErrors pins that a domain error wrapped with
// context on the way up the stack still maps to its canonical code.
func TestMapErrorUnwrapsWrappedDomainErrors(t *testing.T) {
	tests := []struct {
		name string
		in   error
		want codes.Code
	}{
		{
			name: "single wrap",
			in:   fmt.Errorf("create topic: %w", topic.ErrAlreadyExists),
			want: codes.AlreadyExists,
		},
		{
			name: "double wrap",
			in:   fmt.Errorf("publish: %w", fmt.Errorf("append: %w", offset.ErrOutOfRange)),
			want: codes.OutOfRange,
		},
		{
			name: "wrapped cancellation",
			in:   fmt.Errorf("read: %w", context.Canceled),
			want: codes.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code := status.Code(mapError(tt.in)); code != tt.want {
				t.Errorf("mapError(%v) code = %v, want %v", tt.in, code, tt.want)
			}
		})
	}
}

// TestMapErrorPassesThroughTransportErrors pins that an error that is already a
// gRPC status survives unchanged, so a client disconnect mid-Send keeps its
// cancellation semantics instead of being flattened to Internal.
func TestMapErrorPassesThroughTransportErrors(t *testing.T) {
	in := status.Error(codes.Unavailable, "transport is closing")
	got := mapError(in)
	if got != in {
		t.Errorf("mapError() = %v, want the identical error back", got)
	}
	if code := status.Code(got); code != codes.Unavailable {
		t.Errorf("mapError() code = %v, want %v", code, codes.Unavailable)
	}
	if got.Error() != in.Error() {
		t.Errorf("mapError() message = %q, want %q", got.Error(), in.Error())
	}
}

// TestMapErrorUnknownStatusIsNotPassedThrough pins the one status that does not
// take the pass-through path: codes.Unknown means "not classified", so it falls
// through to the domain switch and lands on Internal.
func TestMapErrorUnknownStatusIsNotPassedThrough(t *testing.T) {
	got := mapError(status.Error(codes.Unknown, "mystery"))
	if code := status.Code(got); code != codes.Internal {
		t.Errorf("mapError() code = %v, want %v", code, codes.Internal)
	}
	if msg := status.Convert(got).Message(); msg != "internal error" {
		t.Errorf("mapError() message = %q, want %q", msg, "internal error")
	}
}

// TestMapErrorHidesInternalDetail pins the Protocol.md §4 promise that an
// unrecognized error never leaks its text to the client.
func TestMapErrorHidesInternalDetail(t *testing.T) {
	secret := "panic: nil deref at /home/user/pulse/internal/secret.go:42"
	got := mapError(errors.New(secret))
	if msg := status.Convert(got).Message(); msg != "internal error" {
		t.Errorf("mapError() message = %q, want %q", msg, "internal error")
	}
	if got.Error() == secret {
		t.Error("mapError() leaked the original error text")
	}
}

// TestMapErrorPreservesDomainDetail pins the other half: a client-caused error
// keeps its explanation, because the client can act on it.
func TestMapErrorPreservesDomainDetail(t *testing.T) {
	in := fmt.Errorf("%w: name %q has an invalid character", topic.ErrInvalidName, "a/b")
	msg := status.Convert(mapError(in)).Message()
	if msg != in.Error() {
		t.Errorf("mapError() message = %q, want %q", msg, in.Error())
	}
}

func TestToMessageAndBack(t *testing.T) {
	id := message.NewEventID(time.UnixMilli(1_700_000_000_000))
	pb := &pulsepb.Message{
		EventId:       id.String(),
		Key:           "route-key",
		Payload:       []byte("hello"),
		Headers:       map[string]string{"a": "1"},
		ContentType:   "application/json",
		CorrelationId: "corr",
		TraceId:       "trace",
		RetryCount:    3,
		TtlMs:         1500,
		Priority:      7,
		SchemaVersion: 2,
	}

	m, err := toMessage(pb)
	if err != nil {
		t.Fatalf("toMessage() error = %v", err)
	}
	if m.EventID != id {
		t.Errorf("EventID = %v, want %v", m.EventID, id)
	}
	if m.Key != pb.Key {
		t.Errorf("Key = %q, want %q", m.Key, pb.Key)
	}
	if string(m.Payload) != "hello" {
		t.Errorf("Payload = %q, want %q", m.Payload, "hello")
	}
	if m.Headers["a"] != "1" {
		t.Errorf("Headers = %v, want a=1", m.Headers)
	}
	if m.TTL != 1500*time.Millisecond {
		t.Errorf("TTL = %v, want 1.5s", m.TTL)
	}
	if m.Priority != 7 {
		t.Errorf("Priority = %d, want 7", m.Priority)
	}
	if m.SchemaVersion != 2 {
		t.Errorf("SchemaVersion = %d, want 2", m.SchemaVersion)
	}

	out := toPBMessage(m)
	if out.EventId != pb.EventId {
		t.Errorf("EventId = %q, want %q", out.EventId, pb.EventId)
	}
	if out.TtlMs != pb.TtlMs {
		t.Errorf("TtlMs = %d, want %d", out.TtlMs, pb.TtlMs)
	}
	if out.CorrelationId != pb.CorrelationId || out.TraceId != pb.TraceId {
		t.Errorf("ids = %q/%q, want %q/%q", out.CorrelationId, out.TraceId, pb.CorrelationId, pb.TraceId)
	}
	if out.RetryCount != pb.RetryCount || out.Priority != pb.Priority {
		t.Errorf("retry/priority = %d/%d, want %d/%d", out.RetryCount, out.Priority, pb.RetryCount, pb.Priority)
	}
	if out.ContentType != pb.ContentType {
		t.Errorf("ContentType = %q, want %q", out.ContentType, pb.ContentType)
	}
}

// TestToMessageEmptyEventID pins that an absent wire event id yields the zero
// EventID rather than a parse error: the broker assigns it at publish time.
func TestToMessageEmptyEventID(t *testing.T) {
	m, err := toMessage(&pulsepb.Message{Payload: []byte("x")})
	if err != nil {
		t.Fatalf("toMessage() error = %v", err)
	}
	if !m.EventID.Zero() {
		t.Errorf("EventID = %v, want zero", m.EventID)
	}
}

func TestToMessageInvalidEventID(t *testing.T) {
	_, err := toMessage(&pulsepb.Message{EventId: "not-a-ulid"})
	if err == nil {
		t.Fatal("toMessage() error = nil, want a parse error")
	}
}

// TestToPBMessageZeroEventIDIsEmpty pins that a zero id is rendered as "" on
// the wire, not as the all-zero ULID string.
func TestToPBMessageZeroEventIDIsEmpty(t *testing.T) {
	pb := toPBMessage(message.Message{Payload: []byte("x")})
	if pb.EventId != "" {
		t.Errorf("EventId = %q, want empty", pb.EventId)
	}
}

// TestToPBMessageSubMillisecondTTLTruncates documents the wire's millisecond
// granularity: a sub-millisecond TTL arrives as 0.
func TestToPBMessageSubMillisecondTTLTruncates(t *testing.T) {
	pb := toPBMessage(message.Message{TTL: 500 * time.Microsecond})
	if pb.TtlMs != 0 {
		t.Errorf("TtlMs = %d, want 0", pb.TtlMs)
	}
}

func TestToPBRecord(t *testing.T) {
	name, err := topic.NewName("orders")
	if err != nil {
		t.Fatalf("NewName() error = %v", err)
	}
	ts := time.UnixMilli(1_700_000_000_123).UTC()
	rec := message.Record{
		Offset:    offset.Offset(42),
		Timestamp: ts,
		Message:   message.Message{Payload: []byte("body"), Key: "k"},
	}

	pb := toPBRecord(name, 3, rec)
	if pb.Topic != "orders" {
		t.Errorf("Topic = %q, want %q", pb.Topic, "orders")
	}
	if pb.Partition != 3 {
		t.Errorf("Partition = %d, want 3", pb.Partition)
	}
	if pb.Offset != 42 {
		t.Errorf("Offset = %d, want 42", pb.Offset)
	}
	if pb.TimestampMs != ts.UnixMilli() {
		t.Errorf("TimestampMs = %d, want %d", pb.TimestampMs, ts.UnixMilli())
	}
	if pb.Message == nil || string(pb.Message.Payload) != "body" {
		t.Errorf("Message = %v, want payload %q", pb.Message, "body")
	}
}

func TestToPBTopic(t *testing.T) {
	name, err := topic.NewName("events")
	if err != nil {
		t.Fatalf("NewName() error = %v", err)
	}
	created := time.UnixMilli(1_600_000_000_000).UTC()
	tp := topic.Topic{
		Name:       name,
		Partitions: 4,
		Config: topic.Config{
			MaxMessageBytes:   1024,
			Retention:         retention.Policy{MaxAge: 2 * time.Hour, MaxBytes: 4096},
			Cleanup:           topic.CleanupPolicy("delete"),
			ReplicationFactor: 1,
		},
		CreatedAt: created,
	}

	pb := toPBTopic(tp)
	if pb.Name != "events" {
		t.Errorf("Name = %q, want %q", pb.Name, "events")
	}
	if pb.Partitions != 4 {
		t.Errorf("Partitions = %d, want 4", pb.Partitions)
	}
	if pb.CreatedAtMs != created.UnixMilli() {
		t.Errorf("CreatedAtMs = %d, want %d", pb.CreatedAtMs, created.UnixMilli())
	}
	if pb.Config == nil {
		t.Fatal("Config = nil, want a config")
	}
	if pb.Config.MaxMessageBytes != 1024 {
		t.Errorf("MaxMessageBytes = %d, want 1024", pb.Config.MaxMessageBytes)
	}
	if pb.Config.RetentionMs != (2 * time.Hour).Milliseconds() {
		t.Errorf("RetentionMs = %d, want %d", pb.Config.RetentionMs, (2 * time.Hour).Milliseconds())
	}
	if pb.Config.RetentionBytes != 4096 {
		t.Errorf("RetentionBytes = %d, want 4096", pb.Config.RetentionBytes)
	}
	if pb.Config.CleanupPolicy != "delete" {
		t.Errorf("CleanupPolicy = %q, want %q", pb.Config.CleanupPolicy, "delete")
	}
	if pb.Config.ReplicationFactor != 1 {
		t.Errorf("ReplicationFactor = %d, want 1", pb.Config.ReplicationFactor)
	}
}

// TestFromTopicConfigNilIsDefaults pins that an omitted config on the wire means
// "broker defaults", not an all-zero config.
func TestFromTopicConfigNilIsDefaults(t *testing.T) {
	got := fromTopicConfig(nil)
	if got != topic.DefaultConfig() {
		t.Errorf("fromTopicConfig(nil) = %+v, want %+v", got, topic.DefaultConfig())
	}
}

func TestFromTopicConfigRoundTrip(t *testing.T) {
	want := topic.Config{
		MaxMessageBytes:   2048,
		Retention:         retention.Policy{MaxAge: 30 * time.Minute, MaxBytes: 8192},
		Cleanup:           topic.CleanupPolicy("compact"),
		ReplicationFactor: 3,
	}
	got := fromTopicConfig(toPBTopicConfig(want))
	if got != want {
		t.Errorf("round-tripped config = %+v, want %+v", got, want)
	}
}

func TestToBrokerState(t *testing.T) {
	tests := []struct {
		in   broker.State
		want pulsepb.BrokerState
	}{
		{in: broker.StateStarting, want: pulsepb.BrokerState_BROKER_STATE_STARTING},
		{in: broker.StateRecovering, want: pulsepb.BrokerState_BROKER_STATE_RECOVERING},
		{in: broker.StateRunning, want: pulsepb.BrokerState_BROKER_STATE_RUNNING},
		{in: broker.StateDraining, want: pulsepb.BrokerState_BROKER_STATE_DRAINING},
		{in: broker.StateStopping, want: pulsepb.BrokerState_BROKER_STATE_STOPPING},
		{in: broker.StateStopped, want: pulsepb.BrokerState_BROKER_STATE_STOPPED},
		{in: broker.State(99), want: pulsepb.BrokerState_BROKER_STATE_UNSPECIFIED},
	}
	for _, tt := range tests {
		if got := toBrokerState(tt.in); got != tt.want {
			t.Errorf("toBrokerState(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestToStartOffset(t *testing.T) {
	if got := toStartOffset(nil); got != nil {
		t.Errorf("toStartOffset(nil) = %v, want nil", got)
	}
	v := int64(17)
	got := toStartOffset(&v)
	if got == nil {
		t.Fatal("toStartOffset(&17) = nil, want a pointer")
	}
	if *got != offset.Offset(17) {
		t.Errorf("toStartOffset(&17) = %v, want 17", *got)
	}
	// The result must not alias the caller's variable.
	v = 99
	if *got != offset.Offset(17) {
		t.Errorf("toStartOffset result changed with the caller's variable: %v", *got)
	}
}
