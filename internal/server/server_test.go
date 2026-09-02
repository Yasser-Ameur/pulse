package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Yasser-Ameur/pulse/internal/infrastructure/config"
)

// freePort returns an address bound to an ephemeral, currently-free TCP
// port, for handing to server.Run without server.Run and this test racing
// over ownership of the listener itself.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func TestRunServesUntilSIGTERM(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("syscall.SIGTERM to self is not supported on windows")
	}

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ListenAddr = freePort(t)
	cfg.MonitorAddr = freePort(t)
	cfg.ShutdownGrace = config.Duration(2 * time.Second)

	done := make(chan error, 1)
	go func() { done <- Run(cfg) }()

	// Wait for the monitor to answer /healthz before signaling shutdown, so
	// this also pins that Run brings the monitor up while serving.
	healthURL := "http://" + cfg.MonitorAddr + "/healthz"
	require.Eventually(t, func() bool {
		resp, err := http.Get(healthURL) //nolint:noctx // short-lived polling client, no request-scoped deadline needed
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond, "monitor /healthz never answered 200")

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within the shutdown grace period after SIGTERM")
	}
}

// TestRunReturnsErrorOnDataDirCreationFailure pins that Run fails fast, before
// opening any listener, when its data directory cannot be created (here,
// because a plain file already occupies that path).
func TestRunReturnsErrorOnDataDirCreationFailure(t *testing.T) {
	parent := t.TempDir()
	blocked := filepath.Join(parent, "blocked")
	require.NoError(t, os.WriteFile(blocked, []byte("x"), 0o600))

	cfg := config.Default()
	cfg.DataDir = filepath.Join(blocked, "data") // blocked is a file, not a dir
	cfg.ListenAddr = freePort(t)
	cfg.MonitorAddr = freePort(t)

	require.Error(t, Run(cfg))
}

// TestRunReturnsErrorOnInvalidTLSConfig pins that a bad TLS certificate path
// fails Run before either listener opens, and that it cleans up the broker it
// had already started.
func TestRunReturnsErrorOnInvalidTLSConfig(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ListenAddr = freePort(t)
	cfg.MonitorAddr = freePort(t)
	cfg.TLS = config.TLS{CertFile: "/does/not/exist.pem", KeyFile: "/does/not/exist.key"}

	require.Error(t, Run(cfg))
}

// TestRunReturnsErrorOnListenAddrInUse pins that Run surfaces the gRPC
// listener's bind error rather than hanging, by occupying the address first.
func TestRunReturnsErrorOnListenAddrInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ListenAddr = ln.Addr().String()
	cfg.MonitorAddr = freePort(t)

	require.Error(t, Run(cfg))
}

// writeSelfSignedCert generates an ephemeral self-signed certificate and
// writes its PEM-encoded cert and key to files in t.TempDir(), for exercising
// buildTLSConfig's file-loading path without a real CA.
func writeSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
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
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certFile, keyFile
}

func TestBuildTLSConfigNilWhenUnset(t *testing.T) {
	tlsCfg, err := buildTLSConfig(config.TLS{})
	require.NoError(t, err)
	require.Nil(t, tlsCfg)
}

func TestBuildTLSConfigMissingCertFile(t *testing.T) {
	_, err := buildTLSConfig(config.TLS{CertFile: "/does/not/exist.pem", KeyFile: "/does/not/exist.key"})
	require.Error(t, err)
}

func TestBuildTLSConfigMissingClientCAFile(t *testing.T) {
	certFile, keyFile := writeSelfSignedCert(t)
	_, err := buildTLSConfig(config.TLS{CertFile: certFile, KeyFile: keyFile, ClientCAFile: "/does/not/exist.pem"})
	require.Error(t, err)
}

func TestBuildTLSConfigLoadsCertAndRequiresClientCert(t *testing.T) {
	certFile, keyFile := writeSelfSignedCert(t)
	tlsCfg, err := buildTLSConfig(config.TLS{CertFile: certFile, KeyFile: keyFile})
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	require.Len(t, tlsCfg.Certificates, 1)
	require.Equal(t, tls.NoClientCert, tlsCfg.ClientAuth)

	tlsCfgMTLS, err := buildTLSConfig(config.TLS{CertFile: certFile, KeyFile: keyFile, ClientCAFile: certFile})
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, tlsCfgMTLS.ClientAuth)
}
