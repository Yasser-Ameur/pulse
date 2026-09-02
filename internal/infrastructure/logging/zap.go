// Package logging provides implementations of the Logger port behind zap.
package logging

import (
	"strings"

	"github.com/Yasser-Ameur/pulse/internal/application/ports"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapLogger adapts a zap.Logger to ports.Logger.
type ZapLogger struct {
	z *zap.Logger
}

// NewZapLogger builds a zap-backed logger. level is one of debug, info, warn,
// error (case-insensitive); development enables human-readable, colorized
// output for local use, otherwise output is JSON for production logs.
func NewZapLogger(level string, development bool) (*ZapLogger, error) {
	var cfg zap.Config
	if development {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	cfg.Level = zap.NewAtomicLevelAt(parseLevel(level))
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	z, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	return &ZapLogger{z: z}, nil
}

// MustNewZapLogger builds a zap logger and panics on failure; for composition
// roots and tests that can never fail to build a logger.
func MustNewZapLogger(level string, development bool) *ZapLogger {
	l, err := NewZapLogger(level, development)
	if err != nil {
		panic(err)
	}
	return l
}

// parseLevel maps a human level name to a zapcore.Level.
func parseLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// Debug implements ports.Logger.
func (l *ZapLogger) Debug(msg string, fields ...ports.Field) {
	l.z.Debug(msg, toZapFields(fields)...)
}

// Info implements ports.Logger.
func (l *ZapLogger) Info(msg string, fields ...ports.Field) {
	l.z.Info(msg, toZapFields(fields)...)
}

// Warn implements ports.Logger.
func (l *ZapLogger) Warn(msg string, fields ...ports.Field) {
	l.z.Warn(msg, toZapFields(fields)...)
}

// Error implements ports.Logger.
func (l *ZapLogger) Error(msg string, fields ...ports.Field) {
	l.z.Error(msg, toZapFields(fields)...)
}

// With implements ports.Logger.
func (l *ZapLogger) With(fields ...ports.Field) ports.Logger {
	return &ZapLogger{z: l.z.With(toZapFields(fields)...)}
}

// Sync flushes buffered log output. It is safe to call more than once; the
// first error is the meaningful one (mirroring zap.Logger.Sync semantics).
func (l *ZapLogger) Sync() error { return l.z.Sync() }

// Close flushes buffered log output before process exit.
func (l *ZapLogger) Close() error { return l.z.Sync() }

// toZapFields converts ports.Field values to zap fields.
func toZapFields(fields []ports.Field) []zap.Field {
	out := make([]zap.Field, 0, len(fields))
	for _, f := range fields {
		out = append(out, zap.Any(f.Key, f.Value))
	}
	return out
}

// NopLogger is a Logger that discards every message. It is used where the
// application requires a logger but callers do not care about output.
type NopLogger struct{}

// NewNopLogger returns a logger that discards output.
func NewNopLogger() *NopLogger { return &NopLogger{} }

// Debug implements ports.Logger.
func (NopLogger) Debug(string, ...ports.Field) {}

// Info implements ports.Logger.
func (NopLogger) Info(string, ...ports.Field) {}

// Warn implements ports.Logger.
func (NopLogger) Warn(string, ...ports.Field) {}

// Error implements ports.Logger.
func (NopLogger) Error(string, ...ports.Field) {}

// With implements ports.Logger.
func (l NopLogger) With(...ports.Field) ports.Logger { return l }
