// Package testutil builds in-process brokers for integration tests. It
// assembles the same composition root as internal/server but on an ephemeral
// listener, so tests can run publish/consume/ack and restart scenarios against
// the real gRPC transport without spawning processes.
package testutil

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/application/services"
	grpctransport "github.com/pulse-stream/pulse/internal/infrastructure/grpc"
	"github.com/pulse-stream/pulse/internal/infrastructure/logging"
	"github.com/pulse-stream/pulse/internal/infrastructure/metrics"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/log"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/metadata"
	"github.com/pulse-stream/pulse/internal/infrastructure/timeutil"
)

// Options configures an in-process broker instance.
type Options struct {
	// Dir is the data directory; empty means a fresh temp dir per instance.
	Dir string
	// Logger overrides the nop logger, e.g. for debugging.
	Logger ports.Logger
}

// Instance is a running broker with its gRPC transport.
type Instance struct {
	Addr string
	Dir  string

	app *services.Broker
	srv *grpctransport.Server
	ln  net.Listener
}

// Start assembles and starts a broker plus gRPC server, returning the bound
// address. The instance owns its data directory unless Dir was provided.
func Start(t *testing.T, opts Options) *Instance {
	t.Helper()

	dir := opts.Dir
	if dir == "" {
		dir = t.TempDir()
	}
	logger := opts.Logger
	if logger == nil {
		logger = logging.NewNopLogger()
	}

	meta, err := metadata.OpenPebble(dir + "/meta")
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	factory := log.NewFactory(dir+"/topics", log.Config{}, logger)
	app := services.NewBroker(services.BrokerOptions{
		MetadataStore: meta,
		LogFactory:    factory,
		Clock:         timeutil.SystemClock{},
		Logger:        logger,
		Metrics:       metrics.NoopRecorder{},
		ListenAddr:    "127.0.0.1:0",
		Version:       "test",
		ReadLimit:     512,
		ReadMaxBytes:  1 << 20,
	})
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("start broker: %v", err)
	}

	srv := grpctransport.NewServer(app, timeutil.SystemClock{}, grpctransport.Options{
		GraceTimeout: 5 * time.Second,
		Logger:       logger,
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()

	inst := &Instance{
		Addr: ln.Addr().String(),
		Dir:  dir,
		app:  app,
		srv:  srv,
		ln:   ln,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.GracefulStop(ctx)
		_ = app.Shutdown(ctx)
	})
	return inst
}

// Restart stops the instance (draining transport and broker) and starts a fresh
// broker over the same data directory, verifying durability.
func (i *Instance) Restart(t *testing.T) *Instance {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	i.srv.GracefulStop(ctx)
	if err := i.app.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	_ = i.ln.Close()
	return Start(t, Options{Dir: i.Dir})
}
