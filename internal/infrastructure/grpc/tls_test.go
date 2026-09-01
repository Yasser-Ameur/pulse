package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	grpclib "google.golang.org/grpc"

	"github.com/pulse-stream/pulse/internal/application/services"
	"github.com/pulse-stream/pulse/internal/infrastructure/logging"
	"github.com/pulse-stream/pulse/internal/infrastructure/metrics"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/log"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/metadata"
	"github.com/pulse-stream/pulse/internal/infrastructure/timeutil"
	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
)

// selfSignedCert generates an ephemeral self-signed certificate valid for
// "127.0.0.1", for exercising server TLS without touching the filesystem or
// a real CA.
func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "pulse-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate() error = %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate() error = %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}
}

// newTestBroker builds a minimal running broker for a transport-level test.
func newTestBroker(t *testing.T) *services.Broker {
	t.Helper()
	dir := t.TempDir()
	logger := logging.NewNopLogger()
	meta, err := metadata.OpenPebble(dir + "/meta")
	if err != nil {
		t.Fatalf("OpenPebble() error = %v", err)
	}
	factory := log.NewFactory(dir, log.Config{}, logger)
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
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })
	return app
}

// TestServerTLS pins that a server configured with TLS accepts a client
// dialing with matching credentials and refuses a client dialing insecure.
func TestServerTLS(t *testing.T) {
	cert := selfSignedCert(t)
	app := newTestBroker(t)

	srv := NewServer(app, timeutil.SystemClock{}, Options{
		GraceTimeout: 5 * time.Second,
		TLS: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.GracefulStop(context.Background()) })

	pool := x509.NewCertPool()
	pool.AddCert(cert.Leaf)
	creds := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "127.0.0.1"})

	conn, err := grpclib.NewClient(ln.Addr().String(), grpclib.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pulsepb.NewBrokerClient(conn)
	if _, err := client.BrokerInfo(context.Background(), &pulsepb.BrokerInfoRequest{}); err != nil {
		t.Errorf("BrokerInfo() over TLS error = %v, want nil", err)
	}

	insecureConn, err := grpclib.NewClient(ln.Addr().String(), grpclib.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = insecureConn.Close() })
	insecureClient := pulsepb.NewBrokerClient(insecureConn)
	if _, err := insecureClient.BrokerInfo(context.Background(), &pulsepb.BrokerInfoRequest{}); err == nil {
		t.Error("BrokerInfo() over insecure dial error = nil, want a handshake failure")
	}
}
