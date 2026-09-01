// Package server is the composition root: it wires configuration,
// infrastructure, the application facade, and the gRPC transport together and
// drives the documented lifecycle (docs/Concurrency.md §2, §6).
//
// Only this package may assemble implementations behind ports; nothing below
// it may refer to this package.
package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/application/services"
	"github.com/pulse-stream/pulse/internal/infrastructure/config"
	grpctransport "github.com/pulse-stream/pulse/internal/infrastructure/grpc"
	"github.com/pulse-stream/pulse/internal/infrastructure/logging"
	"github.com/pulse-stream/pulse/internal/infrastructure/metrics"
	"github.com/pulse-stream/pulse/internal/infrastructure/monitor"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/log"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/metadata"
	"github.com/pulse-stream/pulse/internal/infrastructure/timeutil"
)

// Version is the broker release version reported by BrokerInfo.
var Version = "dev"

// Run assembles the broker from cfg and blocks until the process is signaled,
// then performs the documented shutdown sequence.
func Run(cfg config.Config) error {
	logger, err := logging.NewZapLogger(cfg.LogLevel, cfg.Development)
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()

	clock := timeutil.SystemClock{}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics.RegisterBrokerInfo(reg, Version)
	recorder := metrics.NewPrometheusRecorder(reg)

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir %s: %w", cfg.DataDir, err)
	}

	meta, err := metadata.OpenPebble(cfg.DataDir + "/meta")
	if err != nil {
		return err
	}

	factory := log.NewFactory(cfg.DataDir, log.Config{
		MaxSegmentBytes: cfg.Storage.SegmentMaxBytes,
		IndexInterval:   cfg.Storage.IndexIntervalBytes,
		SyncMode:        log.SyncMode(cfg.Storage.SyncMode),
		SyncInterval:    cfg.Storage.SyncInterval.Duration()}, logger)

	app := services.NewBroker(services.BrokerOptions{
		MetadataStore:     meta,
		LogFactory:        factory,
		Clock:             clock,
		Logger:            logger,
		Metrics:           recorder,
		ListenAddr:        cfg.ListenAddr,
		Version:           Version,
		ReadLimit:         cfg.Subscribe.ReadLimit,
		ReadMaxBytes:      cfg.Subscribe.ReadMaxBytes,
		RetentionInterval: cfg.Storage.RetentionInterval.Duration(),
	})

	if err := app.Start(context.Background()); err != nil {
		_ = meta.Close()
		return err
	}
	recorder.SetUp(true)

	var monSrv *http.Server
	if cfg.MonitorAddr != "" {
		monLn, err := net.Listen("tcp", cfg.MonitorAddr)
		if err != nil {
			_ = app.Shutdown(context.Background())
			return err
		}
		startedAt := app.BrokerInfo().StartedAt
		monSrv = &http.Server{
			Handler:           monitor.New(app, Version, startedAt, reg),
			ReadHeaderTimeout: 5 * time.Second,
		}
		logger.Info("monitor server listening", ports.Field{Key: "address", Value: cfg.MonitorAddr})
		go func() { _ = monSrv.Serve(monLn) }()
	}

	tlsConfig, err := buildTLSConfig(cfg.TLS)
	if err != nil {
		_ = app.Shutdown(context.Background())
		return err
	}

	transport := grpctransport.NewServer(app, clock, grpctransport.Options{
		MaxRecvMsgSize: cfg.MaxRecvMsgSize,
		MaxSendMsgSize: cfg.MaxSendMsgSize,
		GraceTimeout:   cfg.ShutdownGrace.Duration(),
		Logger:         logger,
		TLS:            tlsConfig,
	})
	transport.SetServing(true)

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		if monSrv != nil {
			_ = monSrv.Shutdown(context.Background())
		}
		_ = app.Shutdown(context.Background())
		return err
	}

	errCh := make(chan error, 1)
	go func() { errCh <- transport.Serve(ln) }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			if monSrv != nil {
				_ = monSrv.Shutdown(context.Background())
			}
			_ = app.Shutdown(context.Background())
			return err
		}
		return nil
	case <-sigCh:
		logger.Info("shutdown signal received")
	}

	recorder.SetUp(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace.Duration())
	defer cancel()
	transport.GracefulStop(shutdownCtx)
	if monSrv != nil {
		_ = monSrv.Shutdown(shutdownCtx)
	}
	if err := app.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown incomplete", ports.Field{Key: "error", Value: err.Error()})
		return err
	}
	return nil
}

// buildTLSConfig builds the gRPC server's tls.Config from the resolved TLS
// settings. It returns nil when TLS is not configured (plaintext).
func buildTLSConfig(cfg config.TLS) (*tls.Config, error) {
	if cfg.CertFile == "" && cfg.KeyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load tls key pair: %w", err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if cfg.ClientCAFile != "" {
		pem, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read tls client ca %s: %w", cfg.ClientCAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse tls client ca %s: no certificates found", cfg.ClientCAFile)
		}
		tlsConfig.ClientCAs = pool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsConfig, nil
}
