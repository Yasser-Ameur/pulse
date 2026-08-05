// Package broker defines broker identity and lifecycle.
//
// Identity (ClusterID, BrokerID, NodeID) and lifecycle (BrokerState) are
// specified in Phase 1 even though clustering is not. Later phases extend
// these concepts with leader/follower roles instead of introducing new ones.
package broker

import (
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// Sentinel errors returned by broker operations.
var (
	// ErrInvalidTransition reports an illegal lifecycle transition.
	ErrInvalidTransition = errors.New("invalid broker state transition")
	// ErrNotRunning reports that an operation requires a running broker.
	ErrNotRunning = errors.New("broker not running")
	// ErrInvalidAddress reports a missing or malformed advertised address.
	ErrInvalidAddress = errors.New("invalid broker address")
)

// ClusterID is the unique identifier of a Pulse cluster. It is a ULID,
// generated on first start and persisted in the metadata store.
type ClusterID string

// BrokerID uniquely identifies a broker within its cluster. It is a ULID,
// generated on first start and persisted in the metadata store. The name
// mirrors the frozen domain vocabulary (docs/Repository.md) to disambiguate
// from ClusterID and NodeID.
type BrokerID string //nolint:revive // BrokerID is the documented domain term; renaming to ID would be less explicit.

// NodeID uniquely identifies the physical node a broker runs on. It equals the
// BrokerID in single-node deployments and diverges once multiple brokers share
// a node.
type NodeID string

// NewClusterID generates a new cluster identifier carrying the given time.
func NewClusterID(now time.Time) ClusterID {
	return ClusterID(ulid.MustNewDefault(now).String())
}

// NewBrokerID generates a new broker identifier carrying the given time.
func NewBrokerID(now time.Time) BrokerID {
	return BrokerID(ulid.MustNewDefault(now).String())
}

// State is the broker lifecycle state machine.
type State int

const (
	// StateStarting is the initial state: configuration loaded, metadata
	// store opening.
	StateStarting State = iota
	// StateRecovering is the startup phase: logs opening, torn writes being
	// truncated.
	StateRecovering
	// StateRunning is the steady state serving requests.
	StateRunning
	// StateDraining rejects new work while in-flight work completes.
	StateDraining
	// StateStopping releases resources.
	StateStopping
	// StateStopped reports resources released; the process may exit.
	StateStopped
)

// String returns the canonical name of the state.
func (s State) String() string {
	switch s {
	case StateStarting:
		return "starting"
	case StateRecovering:
		return "recovering"
	case StateRunning:
		return "running"
	case StateDraining:
		return "draining"
	case StateStopping:
		return "stopping"
	case StateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// transitions is the allowed transition table.
var transitions = map[State][]State{
	StateStarting:   {StateRecovering, StateStopping},
	StateRecovering: {StateRunning, StateStopping},
	StateRunning:    {StateDraining, StateStopping},
	StateDraining:   {StateStopping},
	StateStopping:   {StateStopped},
	StateStopped:    {},
}

// CanTransitionTo reports whether a transition from s to next is legal.
func (s State) CanTransitionTo(next State) bool {
	for _, allowed := range transitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// Broker is the aggregate describing a broker instance.
type Broker struct {
	// ClusterID identifies the cluster this broker belongs to.
	ClusterID ClusterID
	// BrokerID uniquely identifies this broker within the cluster.
	BrokerID BrokerID
	// NodeID uniquely identifies the physical node.
	NodeID NodeID
	// State is the current lifecycle state.
	State State
	// Address is the advertised gRPC address clients connect to.
	Address string
	// Version is the broker release version.
	Version string
	// StartedAt is when the broker transitioned to Running; zero until then.
	StartedAt time.Time
}

// TransitionTo advances the broker to next, rejecting illegal transitions.
func (b *Broker) TransitionTo(next State) error {
	if !b.State.CanTransitionTo(next) {
		return fmt.Errorf("%w: %s to %s", ErrInvalidTransition, b.State, next)
	}
	b.State = next
	return nil
}
