package main

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulse-stream/pulse/internal/server"
)

// TestMainVersionFlag drives main() itself with --version, the one RunE path
// that returns nil without touching os.Exit, so it is safe to call in the
// test process. The error branch (bad --config) calls os.Exit(1) directly and
// is not reachable from an in-process test without killing the runner; it is
// covered indirectly by config.Load's and server.Run's own tests, which own
// every failure this wiring can propagate.
func TestMainVersionFlag(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{"pulse-server", "--version"}

	r, w, err := os.Pipe()
	require.NoError(t, err)
	origStdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	main()

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Contains(t, string(out), server.Version)
}
