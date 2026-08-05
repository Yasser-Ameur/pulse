package client

import (
	"time"

	"github.com/pulse-stream/pulse/internal/domain/broker"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/retention"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

// toPBMessage converts a domain Message to the wire type, dropping the event id
// (broker-assigned on publish).
func toPBMessage(m message.Message) *pulsepb.Message {
	return &pulsepb.Message{
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
}

// fromPBRecord converts a wire Record back to the domain model, attaching the
// topic it was read from.
func fromPBRecord(name topic.Name, pb *pulsepb.Record) message.Record {
	r := message.Record{
		Offset:    offset.Offset(pb.Offset),
		Timestamp: time.UnixMilli(pb.TimestampMs),
	}
	if pb.Message != nil {
		r.Message = message.Message{
			EventID:       message.EventID{},
			Key:           pb.Message.Key,
			Payload:       pb.Message.Payload,
			Headers:       message.Headers(pb.Message.Headers),
			ContentType:   pb.Message.ContentType,
			CorrelationID: pb.Message.CorrelationId,
			TraceID:       pb.Message.TraceId,
			RetryCount:    pb.Message.RetryCount,
			TTL:           time.Duration(pb.Message.TtlMs) * time.Millisecond,
			Priority:      pb.Message.Priority,
			SchemaVersion: pb.Message.SchemaVersion,
		}
		if pb.Message.EventId != "" {
			r.Message.EventID, _ = message.ParseEventID(pb.Message.EventId)
		}
	}
	return r
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

// fromPBTopic converts a wire Topic to the domain model.
func fromPBTopic(pb *pulsepb.Topic) (topic.Topic, error) {
	name, err := topic.NewName(pb.Name)
	if err != nil {
		return topic.Topic{}, err
	}
	cfg := topic.Config{
		MaxMessageBytes: pb.Config.GetMaxMessageBytes(),
		Retention: retention.Policy{
			MaxAge:   time.Duration(pb.Config.GetRetentionMs()) * time.Millisecond,
			MaxBytes: pb.Config.GetRetentionBytes(),
		},
		Cleanup:           topic.CleanupPolicy(pb.Config.GetCleanupPolicy()),
		ReplicationFactor: pb.Config.GetReplicationFactor(),
	}
	return topic.Topic{
		Name:       name,
		Partitions: int(pb.Partitions),
		Config:     cfg,
		CreatedAt:  time.UnixMilli(pb.CreatedAtMs),
	}, nil
}

// fromBrokerState maps the wire enum to the domain lifecycle state.
func fromBrokerState(s pulsepb.BrokerState) broker.State {
	switch s {
	case pulsepb.BrokerState_BROKER_STATE_STARTING:
		return broker.StateStarting
	case pulsepb.BrokerState_BROKER_STATE_RECOVERING:
		return broker.StateRecovering
	case pulsepb.BrokerState_BROKER_STATE_RUNNING:
		return broker.StateRunning
	case pulsepb.BrokerState_BROKER_STATE_DRAINING:
		return broker.StateDraining
	case pulsepb.BrokerState_BROKER_STATE_STOPPING:
		return broker.StateStopping
	case pulsepb.BrokerState_BROKER_STATE_STOPPED:
		return broker.StateStopped
	default:
		return broker.State(0)
	}
}
