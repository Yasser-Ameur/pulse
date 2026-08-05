// Package metrics provides implementations of the MetricsRecorder port.
//
// Phase 1 ships only the no-op recorder; Prometheus and OpenTelemetry recorders
// are additional implementations of the same port.
package metrics

import (
	"time"

	"github.com/pulse-stream/pulse/internal/application/ports"
)

// NoopRecorder is the MetricsRecorder implementation used in Phase 1. It
// records nothing; it exists so the application layer never branches on a nil
// recorder and so the observability seam stays exercised.
type NoopRecorder struct{}

// RecordPublish implements ports.MetricsRecorder.
func (NoopRecorder) RecordPublish(int, int) {}

// RecordConsume implements ports.MetricsRecorder.
func (NoopRecorder) RecordConsume(int, int) {}

// RecordPublishLatency implements ports.MetricsRecorder.
func (NoopRecorder) RecordPublishLatency(time.Duration) {}

// RecordConsumeLatency implements ports.MetricsRecorder.
func (NoopRecorder) RecordConsumeLatency(time.Duration) {}

// RecordBytesWritten implements ports.MetricsRecorder.
func (NoopRecorder) RecordBytesWritten(int64) {}

// RecordBytesRead implements ports.MetricsRecorder.
func (NoopRecorder) RecordBytesRead(int64) {}

var _ ports.MetricsRecorder = NoopRecorder{}
