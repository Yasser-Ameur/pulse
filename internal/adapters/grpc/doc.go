// Package grpc implements the gRPC service handlers for the Broker and PubSub
// services, translating between the pulse.v1 wire types (pkg/api/pulse/v1)
// and the domain model. It also owns the domain-error to canonical gRPC code
// mapping (docs/Protocol.md §4).
//
// The handlers are thin: they convert, validate, delegate to the application
// facade, and map errors. No business rules live here.
package grpc
