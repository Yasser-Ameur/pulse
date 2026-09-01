// Package cli implements the pulse-cli: a small administrative client over the
// pulse.v1 gRPC protocol (docs/Protocol.md). It uses the public pkg/client.
package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulse-stream/pulse/pkg/client"
	"github.com/spf13/cobra"
)

const defaultAddr = "127.0.0.1:9090"

// unaryTimeout bounds request/response commands (topics, publish, ack, info).
// subscribe opts out so --forever can run until interrupted.
const unaryTimeout = 30 * time.Second

// Options carries the shared CLI flags.
type Options struct {
	Addr string

	TLSCA         string
	TLSCert       string
	TLSKey        string
	TLSSkipVerify bool
}

// NewRootCmd builds the pulse-cli command tree.
func NewRootCmd() *cobra.Command {
	opts := &Options{}
	root := &cobra.Command{
		Use:   "pulse-cli",
		Short: "Pulse broker command-line client",
	}
	root.PersistentFlags().StringVar(&opts.Addr, "addr", defaultAddr, "broker address")
	root.PersistentFlags().StringVar(&opts.TLSCA, "tls-ca", "", "trust this CA certificate file; its presence enables TLS")
	root.PersistentFlags().StringVar(&opts.TLSCert, "tls-cert", "", "client certificate file for mTLS")
	root.PersistentFlags().StringVar(&opts.TLSKey, "tls-key", "", "client key file for mTLS")
	root.PersistentFlags().BoolVar(&opts.TLSSkipVerify, "tls-skip-verify", false, "skip server certificate verification (dev only)")
	root.AddCommand(
		newInfoCmd(opts),
		newTopicsCmd(opts),
		newPublishCmd(opts),
		newSubscribeCmd(opts),
		newAckCmd(opts),
	)
	return root
}

// Execute runs the CLI and exits on error. The context is canceled on
// SIGINT/SIGTERM and carries no deadline, so `subscribe --forever` runs until
// interrupted; unary commands apply their own bound via unaryContext.
func Execute() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := NewRootCmd().ExecuteContext(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// unaryContext returns a context bounded by unaryTimeout for a one-shot,
// request/response command. The caller must call the returned cancel func.
func unaryContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return context.WithTimeout(cmd.Context(), unaryTimeout)
}

// connect dials the broker for the command duration.
func connect(opts *Options, ctx context.Context) (*client.Client, error) {
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, err
	}
	var dialOpts []client.Option
	if tlsConfig != nil {
		dialOpts = append(dialOpts, client.WithTLS(tlsConfig))
	}
	c, err := client.Dial(opts.Addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", opts.Addr, err)
	}
	go func() {
		<-ctx.Done()
		_ = c.Close()
	}()
	return c, nil
}

// buildTLSConfig builds the client TLS config from the --tls-* flags. It
// returns nil, meaning plaintext, when neither a CA nor --tls-skip-verify was
// given.
func buildTLSConfig(opts *Options) (*tls.Config, error) {
	if opts.TLSCA == "" && !opts.TLSSkipVerify {
		return nil, nil
	}
	cfg := &tls.Config{InsecureSkipVerify: opts.TLSSkipVerify}
	if opts.TLSCA != "" {
		pem, err := os.ReadFile(opts.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("read --tls-ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--tls-ca %s: no certificates found", opts.TLSCA)
		}
		cfg.RootCAs = pool
	}
	if opts.TLSCert != "" || opts.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load --tls-cert/--tls-key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
