// Package cli implements the pulse-cli: a small administrative client over the
// pulse.v1 gRPC protocol (docs/Protocol.md). It shares the domain model with
// the broker through the internal gRPC client.
package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pulse-stream/pulse/internal/infrastructure/grpc/client"
	"github.com/spf13/cobra"
)

const defaultAddr = "127.0.0.1:9090"

// Options carries the shared CLI flags.
type Options struct {
	Addr string
}

// NewRootCmd builds the pulse-cli command tree.
func NewRootCmd() *cobra.Command {
	opts := &Options{}
	root := &cobra.Command{
		Use:   "pulse-cli",
		Short: "Pulse broker command-line client",
	}
	root.PersistentFlags().StringVar(&opts.Addr, "addr", defaultAddr, "broker address")
	root.AddCommand(
		newInfoCmd(opts),
		newTopicsCmd(opts),
		newPublishCmd(opts),
		newSubscribeCmd(opts),
		newAckCmd(opts),
	)
	return root
}

// Execute runs the CLI and exits on error.
func Execute() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := NewRootCmd().ExecuteContext(ctx); err != nil {
		cancel()
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	cancel()
}

// connect dials the broker for the command duration.
func connect(opts *Options, ctx context.Context) (*client.Client, error) {
	c, err := client.Dial(opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", opts.Addr, err)
	}
	go func() {
		<-ctx.Done()
		_ = c.Close()
	}()
	return c, nil
}
