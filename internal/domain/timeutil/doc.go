// Package timeutil defines the Clock port that makes time injectable.
//
// The domain and application layers depend on this interface rather than on
// time.Now directly so that tests can drive behavior deterministically. The
// production implementation lives in internal/infrastructure/timeutil.
package timeutil

import "time"

// Clock is the source of wall-clock time for the broker.
type Clock interface {
	// Now returns the current wall-clock time.
	Now() time.Time
}
