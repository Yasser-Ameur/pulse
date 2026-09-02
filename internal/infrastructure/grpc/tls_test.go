package grpc_test

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

	grpctransport "github.com/Yasser-Ameur/pulse/internal/infrastructure/grpc"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/timeutil"
	"github.com/Yasser-Ameur/pulse/internal/testutil"
	"github.com/Yasser-Ameur/pulse/pkg/api/pulse/v1/pulsepb"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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

// TestServerTLS pins that a server configured with TLS accepts a client
// dialing with matching credentials and refuses a client dialing insecure.
func TestServerTLS(t *testing.T) {
	cert := selfSignedCert(t)
	app := testutil.Start(t, testutil.Options{}).Broker()

	srv := grpctransport.NewServer(app, timeutil.SystemClock{}, grpctransport.Options{
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
