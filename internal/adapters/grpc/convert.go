package grpc

import (
	"time"

	"github.com/pulse-stream/pulse/internal/domain/broker"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/retention"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

// toMessage converts a wire Message to the domain model, parsing the event id
// when present.
func toMessage(pb *pulsepb.Message) (message.Message, error) {
	var id message.EventID
	if pb.EventId != "" {
		var err error
		id, err = message.ParseEventID(pb.EventId)
		if err != nil {
			return message.Message{}, err
		}
	}
	return message.Message{
		EventID:       id,
		Key:           pb.Key,
		Payload:       pb.Payload,
		Headers:       message.Headers(pb.Headers),
		ContentType:   pb.ContentType,
		CorrelationID: pb.CorrelationId,
		TraceID:       pb.TraceId,
		RetryCount:    pb.RetryCount,
		TTL:           time.Duration(pb.TtlMs) * time.Millisecond,
		Priority:      pb.Priority,
		SchemaVersion: pb.SchemaVersion,
	}, nil
}

// toPBMessage converts a domain Message to the wire type. A zero event id is
// rendered as the empty string (broker-assigned on the wire), not as the zero
// ULID.
func toPBMessage(m message.Message) *pulsepb.Message {
	pb := &pulsepb.Message{
		Key:           m.Key,
		Payload:       m.Payload,
		Headers:       m.Headers,
		ContentType:   m.ContentType,
		CorrelationId: m.CorrelationID,
		TraceId:       m.TraceID,
		RetryCount:    m.RetryCount,
		TtlMs:         m.TTL.Milliseconds(),
		Priority:      m.Priority,
		SchemaVersion: m.SchemaVersion,
	}
	if !m.EventID.Zero() {
		pb.EventId = m.EventID.String()
	}
	return pb
}

// toPBRecord converts a domain Record, attaching the topic and partition it was
// read from.
func toPBRecord(name topic.Name, pid int32, r message.Record) *pulsepb.Record {
	return &pulsepb.Record{
		Topic:       name.String(),
		Partition:   pid,
		Offset:      r.Offset.Int64(),
		TimestampMs: r.Timestamp.UnixMilli(),
		Message:     toPBMessage(r.Message),
	}
}

// toPBTopic converts a domain Topic to the wire type.
func toPBTopic(t topic.Topic) *pulsepb.Topic {
	return &pulsepb.Topic{
		Name:        t.Name.String(),
		Partitions:  int32(t.Partitions),
		Config:      toPBTopicConfig(t.Config),
		CreatedAtMs: t.CreatedAt.UnixMilli(),
	}
}

// toPBTopicConfig converts a domain TopicConfig to the wire type.
func toPBTopicConfig(c topic.Config) *pulsepb.TopicConfig {
	return &pulsepb.TopicConfig{
		MaxMessageBytes:   c.MaxMessageBytes,
		RetentionMs:       int64(c.Retention.MaxAge / time.Millisecond),
		RetentionBytes:    c.Retention.MaxBytes,
		CleanupPolicy:     string(c.Cleanup),
		ReplicationFactor: c.ReplicationFactor,
	}
}

// fromTopicConfig converts a wire TopicConfig to the domain model. Nil means
// the broker defaults. Zero-valued limits are normalized by the application
// layer's config validation.
func fromTopicConfig(pb *pulsepb.TopicConfig) topic.Config {
	if pb == nil {
		return topic.DefaultConfig()
	}
	return topic.Config{
		MaxMessageBytes: pb.MaxMessageBytes,
		Retention: retention.Policy{
			MaxAge:   time.Duration(pb.RetentionMs) * time.Millisecond,
			MaxBytes: pb.RetentionBytes,
		},
		Cleanup:           topic.CleanupPolicy(pb.CleanupPolicy),
		ReplicationFactor: pb.ReplicationFactor,
	}
}

// toBrokerState maps the domain broker lifecycle to the wire enum.
func toBrokerState(s broker.State) pulsepb.BrokerState {
	switch s {
	case broker.StateStarting:
		return pulsepb.BrokerState_BROKER_STATE_STARTING
	case broker.StateRecovering:
		return pulsepb.BrokerState_BROKER_STATE_RECOVERING
	case broker.StateRunning:
		return pulsepb.BrokerState_BROKER_STATE_RUNNING
	case broker.StateDraining:
		return pulsepb.BrokerState_BROKER_STATE_DRAINING
	case broker.StateStopping:
		return pulsepb.BrokerState_BROKER_STATE_STOPPING
	case broker.StateStopped:
		return pulsepb.BrokerState_BROKER_STATE_STOPPED
	default:
		return pulsepb.BrokerState_BROKER_STATE_UNSPECIFIED
	}
}

// toStartOffset converts an optional wire start offset to the domain
// subscription's pointer form.
func toStartOffset(v *int64) *offset.Offset {
	if v == nil {
		return nil
	}
	o := offset.Offset(*v)
	return &o
}
