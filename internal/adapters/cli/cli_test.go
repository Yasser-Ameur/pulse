package cli

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pulse-stream/pulse/internal/testutil"
)

// run executes the CLI root command with args against a fresh command tree,
// capturing everything the command writes to os.Stdout (the CLI commands
// print directly to it rather than through cmd.OutOrStdout).
func run(t *testing.T, addr string, args ...string) (string, error) {
	t.Helper()
	root := NewRootCmd()
	root.SetArgs(append([]string{"--addr", addr}, args...))

	r, w, err := os.Pipe()
	require.NoError(t, err)
	origStdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	execErr := root.ExecuteContext(ctx)

	_ = w.Close()
	out, _ := io.ReadAll(r)
	return string(out), execErr
}

func TestCLITopicsCreateListDelete(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})

	out, err := run(t, inst.Addr, "topics", "create", "orders", "--partitions", "3")
	require.NoError(t, err)
	require.Contains(t, out, "created topic orders (3 partitions)")

	out, err = run(t, inst.Addr, "topics", "list")
	require.NoError(t, err)
	require.Contains(t, out, "orders")
	require.Contains(t, out, "partitions=3")

	out, err = run(t, inst.Addr, "topics", "delete", "orders")
	require.NoError(t, err)
	require.Contains(t, out, "deleted topic orders")

	out, err = run(t, inst.Addr, "topics", "list")
	require.NoError(t, err)
	require.Empty(t, strings.TrimSpace(out))
}

func TestCLIPublishRoutesByKey(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	_, err := run(t, inst.Addr, "topics", "create", "orders", "--partitions", "4")
	require.NoError(t, err)

	out, err := run(t, inst.Addr, "publish", "orders", "--key", "user-42", "-m", "hello")
	require.NoError(t, err)
	require.Contains(t, out, "published offset")
}

func TestCLIPublishSubscribeAck(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	_, err := run(t, inst.Addr, "topics", "create", "events", "--partitions", "1")
	require.NoError(t, err)

	out, err := run(t, inst.Addr, "publish", "events", "-m", "payload-1")
	require.NoError(t, err)
	require.Contains(t, out, "published offset 0")

	out, err = run(t, inst.Addr, "subscribe", "events", "--consumer", "c1", "--from", "0")
	require.NoError(t, err)
	require.Contains(t, out, "payload-1")

	out, err = run(t, inst.Addr, "ack", "events", "--consumer", "c1", "--offset", "1")
	require.NoError(t, err)
	require.Contains(t, out, "cursor now 1")
}

func TestCLIInfo(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	out, err := run(t, inst.Addr, "info")
	require.NoError(t, err)
	require.Contains(t, out, "cluster:")
	require.Contains(t, out, "version: test")
	require.Contains(t, out, "topics:  0")
}

func TestCLIConnectTLSConfigErrorPropagates(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	_, err := run(t, inst.Addr, "topics", "list", "--tls-ca", "/does/not/exist.pem")
	require.Error(t, err)
}

func TestCLITopicsCreateAlreadyExistsError(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	_, err := run(t, inst.Addr, "topics", "create", "orders")
	require.NoError(t, err)

	_, err = run(t, inst.Addr, "topics", "create", "orders")
	require.Error(t, err)
}

func TestCLITopicsDeleteMissingError(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	_, err := run(t, inst.Addr, "topics", "delete", "missing")
	require.Error(t, err)
}

func TestCLIAckMissingTopicError(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	_, err := run(t, inst.Addr, "ack", "missing", "--consumer", "c1", "--offset", "0")
	require.Error(t, err)
}

// TestCLIPublishReadsFromStdin drives the stdin-payload branch of publish:
// with no -m flag and a non-empty, non-terminal stdin, the command reads the
// payload from stdin instead of sending an empty message.
func TestCLIPublishReadsFromStdin(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	_, err := run(t, inst.Addr, "topics", "create", "orders")
	require.NoError(t, err)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	_, err = w.WriteString("stdin-payload")
	require.NoError(t, err)
	require.NoError(t, w.Close())
	origStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = origStdin }()

	out, err := run(t, inst.Addr, "publish", "orders")
	require.NoError(t, err)
	require.Contains(t, out, "published offset")
}

func TestCLITokenFlagDefaultsFromEnv(t *testing.T) {
	t.Setenv("PULSE_TOKEN", "env-token")
	root := NewRootCmd()
	f := root.PersistentFlags().Lookup("token")
	require.NotNil(t, f)
	require.Equal(t, "env-token", f.Value.String())
}

func TestBuildTLSConfigNilWhenUnset(t *testing.T) {
	cfg, err := buildTLSConfig(&Options{})
	require.NoError(t, err)
	require.Nil(t, cfg)
}

func TestBuildTLSConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
	}{
		{"missing ca file", &Options{TLSCA: "/does/not/exist.pem"}},
		{"invalid ca pem", &Options{TLSCA: writeTempFile(t, "not a cert")}},
		{"missing cert/key pair", &Options{TLSSkipVerify: true, TLSCert: "/does/not/exist.pem", TLSKey: "/does/not/exist.key"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildTLSConfig(tt.opts)
			require.Error(t, err)
		})
	}
}

func TestBuildTLSConfigSkipVerify(t *testing.T) {
	cfg, err := buildTLSConfig(&Options{TLSSkipVerify: true})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.True(t, cfg.InsecureSkipVerify)
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "cli-test-*.pem")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = f.WriteString(content)
	require.NoError(t, err)
	return f.Name()
}
