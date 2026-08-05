// Package timeutil provides the production implementation of the Clock port.
package timeutil

import (
	"time"

	domaintimeutil "github.com/pulse-stream/pulse/internal/domain/timeutil"
)

// SystemClock is the production clock backed by the wall clock.
type SystemClock struct{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

var _ domaintimeutil.Clock = SystemClock{}
