# Roadmap

Pulse is built in phases. Every phase is a vertical slice that compiles, is
tested, and keeps the architecture frozen from Architecture.md. This roadmap
maps each future capability to the extension point it plugs into: none of
these require a redesign.

## Phase 1: Core broker

- Single-node broker with durable append-only log.
- Topics: create / delete / list with persisted metadata.
- Publish (batch → offsets), Subscribe (streaming, follow/replay), Ack (cursor).
- gRPC API, CLI, in-process integration tests, CI, full documentation.
- Status: **shipped** (tag `v0.1.0-phase1`).

## Phase 2: Storage engine (in progress)

What Storage.md already specifies but Phase 1 defers:

- **Shipped**: retention sweeper (time/size) over sealed segments.
- **Shipped**: segment snapshots to make recovery constant-time.
- **Shipped**: stress tests, crash-at-any-point equivalence against a
  reference model (payloads and timestamps preserved, offsets contiguous).
- **Shipped**: data-directory layout fix (topic/partition paths).
- **Shipped**: multi-partition topics, 1 to `topic.MaxPartitions` (256) per
  topic, with per-partition ordering and a cursor per `(consumer, topic,
  partition)` (`internal/domain/topic`,
  `internal/application/services/topic_manager.go`). See
  [Guarantees.md](Guarantees.md) §4 and [Client.md](Client.md) for
  `PartitionForKey`.
- **Shipped**: compaction for compacted topics (last-writer-wins per key,
  offsets preserved and never renumbered, tombstones, copy-and-swap commit,
  crash recovery of an interrupted pass), `internal/infrastructure/storage/engine/log/compact.go`.
  See [Storage.md](Storage.md) §8, [Guarantees.md](Guarantees.md) §8, and
  [compaction-design.md](compaction-design.md) for the design and the two
  shipped deviations from it.
- Remaining: read-only memory mapping of sealed segments.

## Phase 3: Networking

- **Shipped**: TLS and mTLS on the gRPC transport (`tls.cert-file`,
  `tls.key-file`, `tls.client-ca-file`), covering both server and client
  (`internal/infrastructure/grpc/server.go`, `pkg/client.WithTLS`). See
  [Configuration.md](Configuration.md) and [Client.md](Client.md).
- **Shipped**: keepalive parameters and enforcement policy, panic-recovering
  interceptors, and reflection (`internal/infrastructure/grpc/server.go`).
- Batching and compression on the wire (reserved flags in the batch format).
- Reconnect semantics in the public client are shipped (`pkg/client.Publish`
  retries on `Unavailable`; `Subscribe` with `Follow: true` resumes
  transparently); broker-side heartbeats remain.
- **Shipped**: token authentication on the gRPC transport (`auth.tokens`,
  `auth.token-file`, `unaryAuthInterceptor`/`streamAuthInterceptor` in
  `internal/infrastructure/grpc/server.go`), off by default. Authorization is
  still all-or-nothing per token; see [Guarantees.md](Guarantees.md).
- Remaining: per-topic authorization; rich flow-control tuning surfaced as
  configuration.

## Phase 4: Consumer groups

- Group coordinator over the metadata plane (the cursor schema generalizes:
  cursor/group/... ).
- Partition assignment and rebalancing on membership change.
- `CommitOffset` promoted from reserved capability to a first-class RPC.
- Retry policies and dead-letter queues via the reserved `__` internal topic
  namespace.

## Phase 5: Cluster

- **etcd/raft, one group per partition** (Redpanda-style): the approved
  consensus model.
- Raft for cluster metadata; leader/follower roles on `BrokerState`.
- Broker discovery, health monitoring, failover.
- Multi-node integration tests (Testcontainers) as a separate CI workflow.

## Phase 6: Observability

- **Shipped**: `ports.MetricsRecorder` → Prometheus adapter
  (`internal/infrastructure/metrics/prometheus.go`), served at `/metrics` on
  the monitor listener alongside `/healthz`, `/readyz`, and `/varz`
  (`internal/infrastructure/monitor/monitor.go`). Structured logging is
  already in place (zap behind `ports.Logger`).
- Remaining: OpenTelemetry tracing via gRPC interceptors.

## Phase 7: SDK

- **Shipped**: the first-class public Go SDK, `pkg/client` (Dial, Publish,
  Subscribe with follow/resume, Ack, TLS, retry with backoff). See
  [Client.md](Client.md). `pulse-cli` and the integration tests now dial
  through it, and the internal gRPC client that used to seed it is deleted.
- Remaining: `sdk/` SDKs for Python and Java.

## Out of scope until explicitly scoped

Exactly-once delivery, schema registry, multi-broker deployment as a supported
operation, public API stability guarantees. Each has reserved wire/storage
capacity today (producer id/sequence, schema_version, replication_factor) so
enabling it later is additive.
