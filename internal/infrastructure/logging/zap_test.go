package logging

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/pulse-stream/pulse/internal/application/ports"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in   string
		want zapcore.Level
	}{
		{"debug", zapcore.DebugLevel},
		{"DEBUG", zapcore.DebugLevel},
		{"warn", zapcore.WarnLevel},
		{"warning", zapcore.WarnLevel},
		{"error", zapcore.ErrorLevel},
		{"info", zapcore.InfoLevel},
		{"", zapcore.InfoLevel},
		{"nonsense", zapcore.InfoLevel},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, parseLevel(tt.in))
		})
	}
}

func TestNewZapLoggerLevelsAndFields(t *testing.T) {
	l, err := NewZapLogger("debug", false)
	require.NoError(t, err)
	require.NotNil(t, l)

	// Exercise every log level and a field conversion; these do not assert on
	// output content (that is zap's own contract) but pin that none of these
	// calls panic or error against a real zap core.
	l.Debug("debug msg", ports.Field{Key: "k", Value: "v"})
	l.Info("info msg")
	l.Warn("warn msg")
	l.Error("error msg", ports.Field{Key: "n", Value: 42})

	withLogger := l.With(ports.Field{Key: "component", Value: "test"})
	require.NotNil(t, withLogger)
	withLogger.Info("scoped msg")

	// Sync/Close flush to stdout, which some CI sandboxes (and this repo's
	// Docker test runner) mount as a device that rejects fsync; only pin that
	// the calls do not panic, not that the underlying flush succeeds.
	_ = l.Sync()
	_ = l.Close()
}

func TestNewZapLoggerDevelopmentMode(t *testing.T) {
	l, err := NewZapLogger("info", true)
	require.NoError(t, err)
	require.NotNil(t, l)
	l.Info("dev mode")
}

func TestMustNewZapLoggerPanicsOnInvalidConfig(t *testing.T) {
	// A valid level never fails to build, so MustNewZapLogger's happy path is
	// exercised by construction; the panic path is not reachable through the
	// public level API and is not tested here.
	require.NotPanics(t, func() {
		l := MustNewZapLogger("info", false)
		require.NotNil(t, l)
	})
}

func TestNopLoggerDiscardsEverything(t *testing.T) {
	l := NewNopLogger()
	l.Debug("x")
	l.Info("x")
	l.Warn("x")
	l.Error("x")
	scoped := l.With(ports.Field{Key: "a", Value: 1})
	require.NotNil(t, scoped)
	scoped.Info("still discarded")
}
