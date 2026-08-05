# Roadmap

Pulse is built in phases. Every phase is a vertical slice that compiles, is
tested, and keeps the architecture frozen from Architecture.md. This roadmap
maps each future capability to the extension point it plugs into — none of
these require a redesign.

## Phase 1 — Core broker (current)

- Single-node broker with durable append-only log.
- Topics: create / delete / list with persisted metadata.
- Publish (batch → offsets), Subscribe (streaming, follow/replay), Ack (cursor).
- gRPC API, CLI, in-process integration tests, CI, full documentation.
- Status: **shipped** (tag `v0.1.0-phase1`).

## Phase 2 — Storage engine

What Storage.md already specifies but Phase 1 defers:

- Retention sweeper (time/size) over sealed segments.
- Compaction for compacted topics (last-writer-wins per key, address space
  preserved).
- Segment snapshots to make recovery constant-time.
- Read-only memory mapping of sealed segments.
- Stress tests: crash-at-any-point equivalence against a reference model.

## Phase 3 — Networking

- Batching and compression on the wire (reserved flags in the batch format).
- Heartbeats and reconnect semantics in the client (`Subscribe`/`Publish`
  streaming RPCs).
- Authentication hooks (`ports.Authentication` + gRPC interceptors) and TLS.
- Rich flow-control tuning surfaced as configuration.

## Phase 4 — Consumer groups

- Group coordinator over the metadata plane (the cursor schema generalizes:
  cursor/group/... ).
- Partition assignment and rebalancing on membership change.
- `CommitOffset` promoted from reserved capability to a first-class RPC.
- Retry policies and dead-letter queues via the reserved `__` internal topic
  namespace.

## Phase 5 — Cluster

- **etcd/raft, one group per partition** (Redpanda-style) — the approved
  consensus model.
- Raft for cluster metadata; leader/follower roles on `BrokerState`.
- Broker discovery, health monitoring, failover.
- Multi-node integration tests (Testcontainers) as a separate CI workflow.

## Phase 6 — Observability

- `ports.MetricsRecorder` → Prometheus adapter; OpenTelemetry tracing via gRPC
  interceptors; structured logging already in place (zap behind `ports.Logger`).

## Phase 7 — SDK

- First-class public SDKs under `pkg/` (Go) and later `sdk/` (Python, Java).
  The internal client (`internal/infrastructure/grpc/client`) is the seed; it
  moves to `pkg/` once the API stabilizes.
- Typed events, batch publish, streaming consumer, automatic reconnect.

## Out of scope until explicitly scoped

Exactly-once delivery, schema registry, multi-broker deployment as a supported
operation, public API stability guarantees. Each has reserved wire/storage
capacity today (producer id/sequence, schema_version, replication_factor) so
enabling it later is additive.
