# Protocol

Pulse's wire contract is defined by protobuf sources in `api/proto/pulse/v1`
and served over gRPC. This document covers the RPC surface, streaming strategy,
versioning, and compatibility rules.

## 1. Package and services

All messages live in package `pulse.v1` (Go import
`github.com/Yasser-Ameur/pulse/pkg/api/pulse/v1/pulsepb`). Two services are
exposed:

| Service | RPC | Kind | Purpose |
|---|---|---|---|
| `Broker` | `CreateTopic` | unary | Create a topic with 1 to `topic.MaxPartitions` (256) partitions. |
| `Broker` | `DeleteTopic` | unary | Delete a topic and its data. |
| `Broker` | `ListTopics` | unary | List topics and configurations. |
| `Broker` | `BrokerInfo` | unary | Cluster/broker identity and state. |
| `PubSub` | `Publish` | unary | Append a batch of messages; returns offsets. |
| `PubSub` | `Subscribe` | server-streaming | Stream records from a cursor or offset. |
| `PubSub` | `Ack` | unary | Advance a consumer's stored cursor. |

Health is served through the **standard** `grpc.health.v1.Health` service so
that `grpc_health_probe` and ecosystem clients work unchanged. `BrokerInfo`
exposes lifecycle state (`BrokerState`) for richer introspection.

**Deliberately absent in Phase 1**: `CommitOffset`, `Nack`, replay-by-timestamp.
Offset commits become meaningful once consumer groups exist; the Ack path
reserves the machinery (a per-consumer durable cursor) that group commits will
generalize. The protocol document reserves these capabilities but does not
expose them prematurely.

## 2. Message model on the wire

`Message` (the publish unit) and `Record` (the delivered unit) are defined in
`model.proto`. `Record` adds the broker-assigned `offset` and `timestamp_ms` to
a `Message`. Key points:

- `event_id` is a string ULID; empty means broker-assigned.
- `payload` is `bytes`; size is bounded by the broker's max message size.
- `headers` is `map<string,string>`, order-insensitive by design.
- Everything else is advisory and passed through.

## 3. Streaming strategy

`Subscribe` is a server-streaming RPC. The client sends one `SubscribeRequest`
and receives a stream of `SubscribeResponse`, each containing a contiguous batch
of `Record`s.

### Flow control

gRPC's HTTP/2 transport provides the flow control. The server writes records
directly to the stream:

- A slow consumer's transport window fills; the handler blocks on `Send`, which
  stalls that subscriber's reader **without** blocking the log or other
  consumers (each subscription has an independent goroutine).
- The consumer's cursor is advanced by explicit `Ack` calls, so a stalled
  consumer never implicitly loses its position.
- There is no unbounded buffering on the server: records are copied out of the
  log under a short read lock and written immediately, so memory grows with the
  transport window, not with the log.

### End-of-log behavior

The `follow` flag distinguishes the two modes:

- `follow = true` (default): the stream stays open; the reader sleeps on the
  log's data-ready notification until new records are appended.
- `follow = false`: the reader returns the log's current contents from the
  requested offset and completes: a replay.

### Cursor resume

If `consumer` is set and `start_offset` is absent, the stream begins at the
consumer's stored cursor (0 if never set). `start_offset` always overrides the
cursor. This gives deterministic replay (`follow=false` + explicit offset) and
resumable consumption (`consumer` + `follow=true`) from the same RPC.

**A stored cursor is the NEXT offset to consume, not the last one consumed.**
The read position is inclusive: a cursor of `N` delivers the record at `N`
first. A consumer that has processed through offset `N` therefore calls
`Ack(N+1)`, and `Ack(N)` redelivers record `N` on the next resume.

The same rule applies to `Ack` itself: the `offset` field of `AckRequest` is
the next offset to consume. `AckResponse.cursor` echoes the stored cursor after
the call. Acks are monotonic: an offset at or below the stored cursor is
ignored and the existing cursor is returned, so a late or duplicated `Ack`
cannot rewind a consumer.

> Note: `AckRequest.Offset` in the generated
> `pkg/api/pulse/v1/pulsepb/pubsub.pb.go` still carries the old comment, "the
> last offset successfully processed". The `.proto` source it is generated from
> has been corrected; the generated file will pick the correction up the next
> time `pulsepb` is regenerated. This section and `Subscriber.Ack` describe the
> implemented behaviour.

Delivery, durability, and duplicate semantics are stated in
[Guarantees.md](Guarantees.md).

## 3a. Authorization metadata

Every RPC on `Broker` and `PubSub` may need to carry a gRPC metadata key,
`authorization`, valued `Bearer <token>`. Whether it is required depends on
the broker's own `auth.tokens` / `auth.token-file` configuration
([Configuration.md](Configuration.md)): with the merged token set empty
(the default) the key is ignored entirely, and with it non-empty the key is
required and checked in constant time on every call
(`authenticate`, `internal/infrastructure/grpc/server.go`). A missing key, a
value that is not `Bearer <token>`, or a token outside the set fails with
`codes.Unauthenticated`. The standard `grpc.health.v1.Health` service and
reflection are exempt regardless of configuration, so a probe never needs a
token. `pkg/client.WithToken` attaches this metadata for you; see
[Client.md](Client.md).

## 4. Errors

Domain errors are mapped to canonical gRPC codes by the adapter:

| Domain error | gRPC code |
|---|---|
| `topic.ErrInvalidName`, `topic.ErrInvalidConfig`, `message.ErrInvalidMessage` | `InvalidArgument` |
| `topic.ErrNotFound`, partition not found | `NotFound` |
| `topic.ErrAlreadyExists` | `AlreadyExists` |
| `offset.ErrOutOfRange` | `OutOfRange` |
| broker not `Running` | `Unavailable` |
| missing or wrong bearer token, when `auth` is configured | `Unauthenticated` |
| storage corruption / internal | `Internal` |
| client context canceled | `Canceled` |

Error messages are stable and machine-readable at the start (e.g. `topic
not found: orders`). No stack traces are leaked to clients.

## 5. Versioning and backward compatibility

- The protocol version is encoded in the package name: `pulse.v1`, then
  `pulse.v2`, etc. Breaking changes are always a new package; the broker may
  serve multiple versions concurrently.
- Within a version, protobuf field semantics are the compatibility contract:
  fields may be added, never removed or repurposed. Servers ignore unknown
  fields; clients tolerate missing optional fields.
- Wire-format versions inside the storage layer (`magic`, `version` bytes in a
  batch) are independent of the protocol version and are managed by the engine
  with explicit upgrade procedures.

## 6. Transport details

- Default listen address `127.0.0.1:9090` (configurable, see
  `internal/infrastructure/config`).
- Max receive/send message sizes are configurable and default to 64 MiB so
  batches are not artificially limited by transport.
- TLS is available and off by default. The server takes `grpc.Creds(credentials.
  NewTLS(tlsConfig))` when `tls.cert-file`/`tls.key-file` are set, with
  `tls.client-ca-file` enabling mTLS (`internal/infrastructure/grpc/server.go`,
  `internal/server/server.go`). The public client dials with the same
  credentials via `client.WithTLS` (`pkg/client/client.go`). See
  [Configuration.md](Configuration.md) and [Client.md](Client.md).
- `pkg/client` (the public Go client) exposes the `Subscribe` stream via a
  callback `func(Record) error`, so the CLI and tests share one consumption
  path; it also retries `Publish` and, with `Follow: true`, `Subscribe` on a
  transient `Unavailable`.
