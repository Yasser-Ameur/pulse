package main

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/infrastructure/config"
	"github.com/spf13/cobra"
)

// healthcheckTimeout bounds the /readyz probe request.
const healthcheckTimeout = 2 * time.Second

// newHealthcheckCmd builds the "healthcheck" subcommand: it resolves the
// monitor address from --config and/or --monitor-addr and probes /readyz,
// exiting 0 on HTTP 200 and 1 otherwise. This is the command a container
// HEALTHCHECK invokes.
func newHealthcheckCmd() *cobra.Command {
	var (
		configPath  string
		monitorAddr string
	)
	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Probe the broker's /readyz endpoint and exit 0 if ready",
		RunE: func(_ *cobra.Command, _ []string) error {
			addr := monitorAddr
			if addr == "" {
				cfg, err := config.Load(configPath)
				if err != nil {
					return err
				}
				addr = cfg.MonitorAddr
			}
			if addr == "" {
				return fmt.Errorf("no monitor-addr resolved from --monitor-addr or --config")
			}
			return checkHealth(healthBaseURL(addr), healthcheckTimeout)
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "path to YAML config file to read monitor-addr from")
	cmd.Flags().StringVar(&monitorAddr, "monitor-addr", "", "monitor address to probe, overriding --config")
	return cmd
}

// healthBaseURL builds the monitor base URL from addr, dialing 127.0.0.1
// instead of 0.0.0.0 (which a client cannot connect to) while keeping the
// port.
func healthBaseURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	if host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// checkHealth GETs baseURL+"/readyz" and returns nil on HTTP 200, or an error
// describing the status code or connection failure otherwise. It is
// independent of config-file parsing so it can be unit-tested against an
// httptest.Server.
func checkHealth(baseURL string, timeout time.Duration) error {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(baseURL + "/readyz")
	if err != nil {
		return fmt.Errorf("readyz request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readyz returned status %d", resp.StatusCode)
	}
	return nil
}
