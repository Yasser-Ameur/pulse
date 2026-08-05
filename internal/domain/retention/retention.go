// Package retention defines the durable time and size limits applied to logs.
//
// Retention policies are persisted as part of topic configuration. Phase 1
// stores and serves them; the sweeper that enforces them arrives with the
// storage engine phase.
package retention

import (
	"errors"
	"fmt"
	"time"
)

// Sentinel errors returned by retention validation.
var (
	// ErrInvalidPolicy is the root of all retention policy errors.
	ErrInvalidPolicy = errors.New("invalid retention policy")
	// ErrNegativeMaxAge reports a negative time-based limit.
	ErrNegativeMaxAge = fmt.Errorf("%w: negative max age", ErrInvalidPolicy)
	// ErrNegativeMaxBytes reports a negative size-based limit.
	ErrNegativeMaxBytes = fmt.Errorf("%w: negative max bytes", ErrInvalidPolicy)
)

// Policy is a set of independent retention limits. A zero value disables the
// corresponding limit.
type Policy struct {
	// MaxAge is the maximum age of a record before it may be deleted. Zero
	// disables time-based retention.
	MaxAge time.Duration
	// MaxBytes is the maximum total size of a log before the oldest segments
	// may be deleted. Zero disables size-based retention.
	MaxBytes int64
}

// Validate reports whether the policy is structurally valid.
func (p Policy) Validate() error {
	if p.MaxAge < 0 {
		return ErrNegativeMaxAge
	}
	if p.MaxBytes < 0 {
		return ErrNegativeMaxBytes
	}
	return nil
}
