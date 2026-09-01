// Package client is the public Go client for the Pulse broker. It speaks the
// pulse.v1 gRPC protocol (docs/Protocol.md) and exposes plain public types, so
// any external Go program can depend on it without importing anything under
// github.com/pulse-stream/pulse/internal.
//
// Dial with WithToken to authenticate against a broker that requires it, and
// call PartitionForKey to route a message to the same partition as every
// other message sharing its key on a multi-partition topic.
//
// # At-least-once delivery
//
// Pulse delivers every record at least once: a crash or a resumed Subscribe
// can redeliver records a consumer already processed, so consumers must be
// idempotent. A consumer acknowledges progress by calling Ack with the next
// offset it wants to read (one past the last record it finished processing),
// not the offset of that last record itself. Subscribe with
// SubscribeOptions.Follow resumes automatically after a transient failure
// from the offset immediately after the last record it delivered, or from
// the caller's cursor if nothing was delivered yet.
package client
