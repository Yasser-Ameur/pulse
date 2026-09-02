package client

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCodeSentinelUnknownCodeReturnsNil(t *testing.T) {
	require.Nil(t, codeSentinel(codes.Internal))
	require.Nil(t, codeSentinel(codes.OK))
}

func TestWrapErrNilIsNil(t *testing.T) {
	require.NoError(t, wrapErr(nil))
}

func TestWrapErrUnmappedCodePassesThrough(t *testing.T) {
	orig := status.Error(codes.Internal, "boom")
	require.Equal(t, orig, wrapErr(orig))
}

func TestShouldResume(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"context canceled never resumes", context.Canceled, false},
		{"deadline exceeded never resumes", context.DeadlineExceeded, false},
		{"unavailable resumes", status.Error(codes.Unavailable, "draining"), true},
		{"a different status code does not resume", status.Error(codes.NotFound, "gone"), false},
		{"a non-status transport error resumes", errors.New("connection reset"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldResume(tt.err))
		})
	}
}

func TestStatusErrorMessageAndUnwrap(t *testing.T) {
	orig := status.Error(codes.NotFound, "topic missing")
	wrapped := wrapErr(orig)

	require.Equal(t, orig.Error(), wrapped.Error())
	require.ErrorIs(t, wrapped, ErrNotFound)
	require.True(t, errors.Is(wrapped, orig))
	require.Equal(t, codes.NotFound, status.Code(wrapped))
}
