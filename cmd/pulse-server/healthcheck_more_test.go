package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func runHealthcheck(t *testing.T, args ...string) error {
	t.Helper()
	cmd := newHealthcheckCmd()
	cmd.SetArgs(args)
	return cmd.Execute()
}

func TestHealthcheckCmdMonitorAddrFlagSucceeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	require.NoError(t, runHealthcheck(t, "--monitor-addr", srv.Listener.Addr().String()))
}

func TestHealthcheckCmdMonitorAddrFlagFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	require.Error(t, runHealthcheck(t, "--monitor-addr", srv.Listener.Addr().String()))
}

func TestHealthcheckCmdReadsMonitorAddrFromConfigFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("monitor-addr: \""+srv.Listener.Addr().String()+"\"\n"), 0o600))

	require.NoError(t, runHealthcheck(t, "--config", path))
}

func TestHealthcheckCmdNoMonitorAddrResolved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("monitor-addr: \"\"\n"), 0o600))

	err := runHealthcheck(t, "--config", path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no monitor-addr resolved")
}

func TestHealthcheckCmdInvalidConfigFilePropagatesError(t *testing.T) {
	require.Error(t, runHealthcheck(t, "--config", "/does/not/exist.yaml"))
}
