package client

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sentinel errors mapped from the gRPC status codes the broker returns.
// errors.Is matches these against any error returned by a Client method;
// status.Code still recovers the original gRPC status from the same error.
var (
	ErrNotFound        = errors.New("pulse: not found")
	ErrAlreadyExists   = errors.New("pulse: already exists")
	ErrInvalidArgument = errors.New("pulse: invalid argument")
	ErrUnavailable     = errors.New("pulse: unavailable")
)

// codeSentinel maps a gRPC code to its exported sentinel, if any.
func codeSentinel(c codes.Code) error {
	switch c {
	case codes.NotFound:
		return ErrNotFound
	case codes.AlreadyExists:
		return ErrAlreadyExists
	case codes.InvalidArgument:
		return ErrInvalidArgument
	case codes.Unavailable:
		return ErrUnavailable
	default:
		return nil
	}
}

// wrapErr maps a gRPC status error to its sentinel while keeping err itself
// reachable through Unwrap, so status.Code(wrapErr(err)) still recovers the
// original code.
func wrapErr(err error) error {
	if err == nil {
		return nil
	}
	sentinel := codeSentinel(status.Code(err))
	if sentinel == nil {
		return err
	}
	return &statusError{sentinel: sentinel, cause: err}
}

type statusError struct {
	sentinel error
	cause    error
}

func (e *statusError) Error() string { return e.cause.Error() }

func (e *statusError) Unwrap() []error { return []error{e.sentinel, e.cause} }
