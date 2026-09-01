package client

import (
	"time"

	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

func toPBMessage(m Message) *pulsepb.Message {
	return &pulsepb.Message{
		Key:           m.Key,
		Payload:       m.Payload,
		Headers:       m.Headers,
		ContentType:   m.ContentType,
		CorrelationId: m.CorrelationID,
		TraceId:       m.TraceID,
	}
}

func fromPBMessage(pb *pulsepb.Message) Message {
	if pb == nil {
		return Message{}
	}
	return Message{
		Key:           pb.Key,
		Payload:       pb.Payload,
		Headers:       pb.Headers,
		ContentType:   pb.ContentType,
		CorrelationID: pb.CorrelationId,
		TraceID:       pb.TraceId,
		EventID:       pb.EventId,
	}
}

func fromPBRecord(topicName string, pb *pulsepb.Record) Record {
	return Record{
		Topic:     topicName,
		Partition: pb.Partition,
		Offset:    pb.Offset,
		Timestamp: time.UnixMilli(pb.TimestampMs),
		Message:   fromPBMessage(pb.Message),
	}
}

func toPBTopicConfig(c TopicConfig) *pulsepb.TopicConfig {
	return &pulsepb.TopicConfig{
		MaxMessageBytes: int64(c.MaxMessageBytes),
		RetentionMs:     c.RetentionMaxAge.Milliseconds(),
		RetentionBytes:  c.RetentionMaxBytes,
		CleanupPolicy:   c.Cleanup,
	}
}

func fromPBTopic(pb *pulsepb.Topic) Topic {
	cfg := TopicConfig{Partitions: int(pb.Partitions)}
	if pb.Config != nil {
		cfg.MaxMessageBytes = int(pb.Config.MaxMessageBytes)
		cfg.RetentionMaxAge = time.Duration(pb.Config.RetentionMs) * time.Millisecond
		cfg.RetentionMaxBytes = pb.Config.RetentionBytes
		cfg.Cleanup = pb.Config.CleanupPolicy
	}
	return Topic{
		Name:      pb.Name,
		Config:    cfg,
		CreatedAt: time.UnixMilli(pb.CreatedAtMs),
	}
}

// brokerStateName maps the wire lifecycle enum to its lowercase name.
func brokerStateName(s pulsepb.BrokerState) string {
	switch s {
	case pulsepb.BrokerState_BROKER_STATE_STARTING:
		return "starting"
	case pulsepb.BrokerState_BROKER_STATE_RECOVERING:
		return "recovering"
	case pulsepb.BrokerState_BROKER_STATE_RUNNING:
		return "running"
	case pulsepb.BrokerState_BROKER_STATE_DRAINING:
		return "draining"
	case pulsepb.BrokerState_BROKER_STATE_STOPPING:
		return "stopping"
	case pulsepb.BrokerState_BROKER_STATE_STOPPED:
		return "stopped"
	default:
		return "unspecified"
	}
}
