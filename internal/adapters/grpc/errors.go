package grpc

import (
	"context"
	"errors"

	"github.com/pulse-stream/pulse/internal/domain/broker"
	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mapError converts a domain error to a canonical gRPC status (docs/Protocol.md
// §4). Unrecognized errors become Internal; no stack traces are leaked.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	// Already-transport errors (e.g. client disconnects during Send) pass
	// through unchanged so cancellation semantics survive the handler.
	if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	case errors.Is(err, topic.ErrInvalidName),
		errors.Is(err, topic.ErrInvalidConfig),
		errors.Is(err, topic.ErrInvalidPartitionCount),
		errors.Is(err, topic.ErrReservedName),
		errors.Is(err, message.ErrInvalidMessage),
		errors.Is(err, consumer.ErrInvalidName):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, topic.ErrNotFound),
		errors.Is(err, partition.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, topic.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, offset.ErrOutOfRange):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, broker.ErrDraining):
		return status.Error(codes.Unavailable, "broker draining")
	case errors.Is(err, broker.ErrNotRunning):
		return status.Error(codes.Unavailable, "broker not running")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
