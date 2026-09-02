package main

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMainHelpFlag drives main() with --help, the one path through
// cli.Execute() that returns without an error (cobra prints usage and stops),
// so it is safe to call in the test process without hitting cli.Execute's
// os.Exit(1) error branch. Every other behaviour main() wires up (dial,
// commands, TLS) is exercised end to end by internal/adapters/cli's own
// tests against a real broker.
func TestMainHelpFlag(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{"pulse-cli", "--help"}

	r, w, err := os.Pipe()
	require.NoError(t, err)
	origStdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	main()

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Contains(t, string(out), "pulse-cli")
}
