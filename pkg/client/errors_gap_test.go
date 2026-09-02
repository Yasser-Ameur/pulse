package client

import (
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

func TestStatusErrorMessageAndUnwrap(t *testing.T) {
	orig := status.Error(codes.NotFound, "topic missing")
	wrapped := wrapErr(orig)

	require.Equal(t, orig.Error(), wrapped.Error())
	require.ErrorIs(t, wrapped, ErrNotFound)
	require.True(t, errors.Is(wrapped, orig))
	require.Equal(t, codes.NotFound, status.Code(wrapped))
}
