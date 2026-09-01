// Package cli implements the pulse-cli: a small administrative client over the
// pulse.v1 gRPC protocol (docs/Protocol.md). It shares the domain model with
// the broker through the internal gRPC client.
package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulse-stream/pulse/internal/infrastructure/grpc/client"
	"github.com/spf13/cobra"
)

const defaultAddr = "127.0.0.1:9090"

// unaryTimeout bounds request/response commands (topics, publish, ack, info).
// subscribe opts out so --forever can run until interrupted.
const unaryTimeout = 30 * time.Second

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
