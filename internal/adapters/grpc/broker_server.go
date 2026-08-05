package grpc

import (
	"context"

	"github.com/pulse-stream/pulse/internal/application/services"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

// BrokerServer implements the pulse.v1 Broker administration service.
type BrokerServer struct {
	pulsepb.UnimplementedBrokerServer
	app *services.Broker
}

// NewBrokerServer builds a BrokerServer delegating to the application facade.
func NewBrokerServer(app *services.Broker) *BrokerServer {
	return &BrokerServer{app: app}
}

// CreateTopic implements pulsepb.BrokerServer.
func (s *BrokerServer) CreateTopic(ctx context.Context, req *pulsepb.CreateTopicRequest) (*pulsepb.CreateTopicResponse, error) {
	t, err := s.app.CreateTopic(ctx, req.Name, fromTopicConfig(req.Config), int(req.Partitions))
	if err != nil {
		return nil, mapError(err)
	}
	return &pulsepb.CreateTopicResponse{Topic: toPBTopic(t)}, nil
}

// DeleteTopic implements pulsepb.BrokerServer.
func (s *BrokerServer) DeleteTopic(ctx context.Context, req *pulsepb.DeleteTopicRequest) (*pulsepb.DeleteTopicResponse, error) {
	if err := s.app.DeleteTopic(ctx, req.Name); err != nil {
		return nil, mapError(err)
	}
	return &pulsepb.DeleteTopicResponse{}, nil
}

// ListTopics implements pulsepb.BrokerServer.
func (s *BrokerServer) ListTopics(ctx context.Context, _ *pulsepb.ListTopicsRequest) (*pulsepb.ListTopicsResponse, error) {
	topics, err := s.app.ListTopics(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	resp := &pulsepb.ListTopicsResponse{Topics: make([]*pulsepb.Topic, 0, len(topics))}
	for _, t := range topics {
		resp.Topics = append(resp.Topics, toPBTopic(t))
	}
	return resp, nil
}

// BrokerInfo implements pulsepb.BrokerServer.
func (s *BrokerServer) BrokerInfo(ctx context.Context, _ *pulsepb.BrokerInfoRequest) (*pulsepb.BrokerInfoResponse, error) {
	info := s.app.BrokerInfo()
	resp := &pulsepb.BrokerInfoResponse{
		ClusterId:   string(info.ClusterID),
		BrokerId:    string(info.BrokerID),
		NodeId:      string(info.NodeID),
		Address:     info.Address,
		State:       toBrokerState(info.State),
		Version:     info.Version,
		StartedAtMs: info.StartedAt.UnixMilli(),
	}
	topics, err := s.app.ListTopics(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	resp.Topics = int32(len(topics))
	return resp, nil
}

var _ pulsepb.BrokerServer = (*BrokerServer)(nil)
