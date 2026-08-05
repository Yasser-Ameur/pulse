// Package producer defines producer identity.
//
// Producer IDs are the key to future idempotent publishing: once exactly-once
// delivery is enabled, a producer id plus sequence number lets the broker
// deduplicate retried publishes. Phase 1 defines the identity so the wire and
// storage formats already carry the reservation.
package producer

// ID is the unique identifier of a producer within the cluster.
type ID string

// String returns the producer identifier.
func (id ID) String() string { return string(id) }
