// Package partition defines partition identity and state.
//
// A partition is an ordered record sequence within a topic. Phase 1 topics
// have exactly one partition, but the model is partition-aware from the start
// so that multi-partition topics require no domain change.
package partition

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by partition operations.
var (
	// ErrNotFound reports that a partition does not exist.
	ErrNotFound = errors.New("partition not found")
	// ErrInvalidID reports an out-of-range partition identifier.
	ErrInvalidID = fmt.Errorf("%w: invalid partition id", ErrNotFound)
)

// ID is a zero-based partition identifier within a topic.
type ID int32

// Valid reports whether the partition id is legal for a topic with the given
// partition count.
func (id ID) Valid(partitions int) bool {
	return partitions > 0 && int(id) >= 0 && int(id) < partitions
}

// Int32 returns the partition id as an int32, the wire representation.
func (id ID) Int32() int32 { return int32(id) }

// State describes the lifecycle of a partition. Future phases (replication,
// leader election) extend it.
type State string

const (
	// StateActive is a partition that is serving reads and writes.
	StateActive State = "active"
)

// Partition is the domain view of a partition: an identity plus its lifecycle
// state. Persistence is delegated to the Log port, so the entity stays free of
// storage concerns.
type Partition struct {
	// ID identifies the partition within its topic.
	ID ID
	// State is the partition lifecycle state.
	State State
}
