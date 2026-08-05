// Package offset defines the immutable position of a record within a log.
//
// Offsets are the foundation of every other guarantee Pulse provides: they are
// assigned in strict append order, are never reused, and uniquely identify a
// record within a partition.
package offset

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by offset operations.
var (
	// ErrOutOfRange reports an offset outside the log's current bounds.
	ErrOutOfRange = errors.New("offset out of range")
	// ErrInvalid reports an offset that is not a legal record position.
	ErrInvalid = fmt.Errorf("%w: invalid offset", ErrOutOfRange)
)

// Offset is a monotonically increasing position in a partition log. The first
// record appended to an empty log receives offset zero.
type Offset int64

// Invalid is the sentinel offset used for unassigned or unspecified positions.
// It is not a valid record position and must not be persisted.
const Invalid Offset = -1

// String returns the decimal representation of the offset.
func (o Offset) String() string { return fmt.Sprintf("%d", int64(o)) }

// Int64 returns the offset as an int64, the representation used on the wire.
func (o Offset) Int64() int64 { return int64(o) }

// Valid reports whether o is a legal record position.
func (o Offset) Valid() bool { return o >= 0 }
