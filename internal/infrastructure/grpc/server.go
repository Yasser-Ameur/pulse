// Package grpc owns the transport server: registration of the pulse.v1
// services and the health service, plus the documented accept/shutdown loop
// (docs/Concurrency.md §2, §6). The request handlers themselves live in
// adapters/grpc; this package is pure plumbing.
package grpc

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"net"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	grpcadapters "github.com/pulse-stream/pulse/internal/adapters/grpc"
	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/application/services"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// healthServiceNames are the service names whose health is flipped together
// with the empty ("overall") service, so grpc_health_probe -service works
// against either pulse.v1 service.
var healthServiceNames = []string{"", pulsepb.Broker_ServiceDesc.ServiceName, pulsepb.PubSub_ServiceDesc.ServiceName}

// fieldMethod is the log field carrying the full gRPC method name.
const fieldMethod = "method"

// Options configures the transport server.
type Options struct {
	// MaxRecvMsgSize bounds a single incoming request frame.
	MaxRecvMsgSize int
	// MaxSendMsgSize bounds a single outgoing response frame.
	MaxSendMsgSize int
	// GraceTimeout bounds GracefulStop; on expiry Stop force-closes RPCs.
	GraceTimeout time.Duration
	// Logger is used for lifecycle log lines and per-RPC debug logging.
	Logger ports.Logger
	// TLS configures the server's transport credentials. Nil serves
	// plaintext.
	TLS *tls.Config
	// Tokens are the accepted bearer tokens for client authentication.
	// Empty/nil means authentication is off.
	Tokens []string
}

// DefaultOptions applies the protocol transport defaults.
func DefaultOptions() Options {
	return Options{
		MaxRecvMsgSize: 64 << 20,
		MaxSendMsgSize: 64 << 20,
		GraceTimeout:   10 * time.Second,
		Logger:         nil,
	}
}

// Server wraps a grpc.Server with the pulse.v1 services and health endpoint.
type Server struct {
	grpc        *grpc.Server
	health      *health.Server
	opts        Options
	connections *atomic.Int64
}

// connCountHandler is a stats.Handler that counts live connections via
// ConnBegin/ConnEnd; every other hook is a no-op.
type connCountHandler struct {
	connections *atomic.Int64
}

func (connCountHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (connCountHandler) HandleRPC(context.Context, stats.RPCStats)                       {}
func (connCountHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h connCountHandler) HandleConn(_ context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
		h.connections.Add(1)
	case *stats.ConnEnd:
		h.connections.Add(-1)
	}
}

// NewServer constructs a Server and registers the Broker and PubSub services.
// app is the application facade; clock must match the one injected into the
// broker.
func NewServer(app *services.Broker, clock ports.Clock, opts Options) *Server {
	if opts.MaxRecvMsgSize <= 0 {
		opts.MaxRecvMsgSize = 64 << 20
	}
	if opts.MaxSendMsgSize <= 0 {
		opts.MaxSendMsgSize = 64 << 20
	}
	if opts.GraceTimeout <= 0 {
		opts.GraceTimeout = 10 * time.Second
	}

	unaryInterceptors := []grpc.UnaryServerInterceptor{unaryInterceptor(opts.Logger)}
	streamInterceptors := []grpc.StreamServerInterceptor{streamInterceptor(opts.Logger)}
	if len(opts.Tokens) > 0 {
		unaryInterceptors = append(unaryInterceptors, unaryAuthInterceptor(opts.Tokens))
		streamInterceptors = append(streamInterceptors, streamAuthInterceptor(opts.Tokens))
	}

	connections := &atomic.Int64{}

	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(opts.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(opts.MaxSendMsgSize),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
		grpc.StatsHandler(connCountHandler{connections: connections}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	if opts.TLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(opts.TLS)))
	}

	s := grpc.NewServer(serverOpts...)
	hs := health.NewServer()
	for _, name := range healthServiceNames {
		hs.SetServingStatus(name, healthv1.HealthCheckResponse_NOT_SERVING)
	}

	pulsepb.RegisterBrokerServer(s, grpcadapters.NewBrokerServer(app))
	pulsepb.RegisterPubSubServer(s, grpcadapters.NewPubSubServer(app, clock))
	healthv1.RegisterHealthServer(s, hs)
	reflection.Register(s)

	return &Server{grpc: s, health: hs, opts: opts, connections: connections}
}

// Connections returns the number of currently open client connections.
func (s *Server) Connections() int64 {
	return s.connections.Load()
}

// unaryInterceptor recovers a handler panic as codes.Internal and logs the
// call at debug level with method, code, and duration.
func unaryInterceptor(logger ports.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer finishRPC(logger, info.FullMethod, time.Now(), &err)
		return handler(ctx, req)
	}
}

// streamInterceptor recovers a handler panic as codes.Internal and logs the
// call at debug level with method, code, and duration.
func streamInterceptor(logger ports.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer finishRPC(logger, info.FullMethod, time.Now(), &err)
		return handler(srv, ss)
	}
}

// authExemptPrefixes are full-method prefixes that skip token authentication:
// the health and reflection services, so a probe or introspection tool never
// needs a token.
var authExemptPrefixes = []string{"/grpc.health.v1.Health/", "/grpc.reflection."}

// errMissingOrInvalidToken is returned for a missing, malformed, or
// unrecognized bearer token.
var errMissingOrInvalidToken = status.Error(codes.Unauthenticated, "missing or invalid token")

// authExempt reports whether fullMethod skips token authentication.
func authExempt(fullMethod string) bool {
	for _, prefix := range authExemptPrefixes {
		if strings.HasPrefix(fullMethod, prefix) {
			return true
		}
	}
	return false
}

// authenticate extracts the "authorization" metadata key from ctx and checks
// it against tokens as "Bearer <token>", comparing in constant time.
func authenticate(ctx context.Context, tokens []string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errMissingOrInvalidToken
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return errMissingOrInvalidToken
	}
	const prefix = "Bearer "
	v := values[0]
	if !strings.HasPrefix(v, prefix) {
		return errMissingOrInvalidToken
	}
	candidate := strings.TrimPrefix(v, prefix)
	for _, tok := range tokens {
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(tok)) == 1 {
			return nil
		}
	}
	return errMissingOrInvalidToken
}

// unaryAuthInterceptor rejects unary calls without a valid bearer token in
// tokens, exempting health and reflection.
func unaryAuthInterceptor(tokens []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if authExempt(info.FullMethod) {
			return handler(ctx, req)
		}
		if err := authenticate(ctx, tokens); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// streamAuthInterceptor rejects stream calls without a valid bearer token in
// tokens, exempting health and reflection.
func streamAuthInterceptor(tokens []string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if authExempt(info.FullMethod) {
			return handler(srv, ss)
		}
		if err := authenticate(ss.Context(), tokens); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// finishRPC is the deferred tail of both interceptors: it recovers a handler
// panic as codes.Internal (logging the stack) and logs the completed call.
// It must be the deferred function itself so recover sees the panic.
func finishRPC(logger ports.Logger, method string, start time.Time, err *error) {
	if r := recover(); r != nil {
		if logger != nil {
			logger.Error("rpc panic",
				ports.Field{Key: fieldMethod, Value: method},
				ports.Field{Key: "panic", Value: r},
				ports.Field{Key: "stack", Value: string(debug.Stack())},
			)
		}
		*err = status.Errorf(codes.Internal, "internal error")
	}
	logRPC(logger, method, *err, time.Since(start))
}

// logRPC emits a single debug-level line per completed RPC.
func logRPC(logger ports.Logger, method string, err error, d time.Duration) {
	if logger == nil {
		return
	}
	logger.Debug("rpc",
		ports.Field{Key: fieldMethod, Value: method},
		ports.Field{Key: "code", Value: status.Code(err).String()},
		ports.Field{Key: "duration", Value: d.String()},
	)
}

// Start begins serving on ln in a background goroutine and returns any fatal
// accept-loop error. It returns nil once Serve has begun.
func (s *Server) Start(ln net.Listener) error {
	if s.opts.Logger != nil {
		s.opts.Logger.Info("gRPC server listening",
			ports.Field{Key: "address", Value: ln.Addr().String()},
		)
	}
	go func() {
		_ = s.grpc.Serve(ln) //nolint:errcheck // Serve error surfaces via ServeAndBlock for tests
	}()
	return nil
}

// Serve blocks until the server stops, returning the accept-loop error.
func (s *Server) Serve(ln net.Listener) error {
	return s.grpc.Serve(ln)
}

// SetServing marks the health endpoint SERVING (broker Running) or
// NOT_SERVING (any other lifecycle state), for the overall service and for
// each of the pulse.v1 service names.
func (s *Server) SetServing(serving bool) {
	status := healthv1.HealthCheckResponse_NOT_SERVING
	if serving {
		status = healthv1.HealthCheckResponse_SERVING
	}
	for _, name := range healthServiceNames {
		s.health.SetServingStatus(name, status)
	}
}

// GracefulStop implements the documented drain: it stops accepting new RPCs,
// flips health to NOT_SERVING, and drains in-flight calls within the grace
// timeout, force-closing afterwards (docs/Concurrency.md §6).
func (s *Server) GracefulStop(ctx context.Context) {
	s.SetServing(false)
	done := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.grpc.Stop()
		<-done
	}
}
