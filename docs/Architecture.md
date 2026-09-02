# Architecture

Pulse is a distributed event streaming platform: durable, ordered, and
replayable delivery of events between independent services. This document is
the frozen architectural contract for the project. It defines the layers, the
domain model, the public interfaces, and the extension points that later phases
(replication, consumer groups, observability, security) must plug into without
structural change.

## 1. System overview

```
   FlowOS                         consumers
   ┌───────────┐                 ┌──────────────────────────┐
   │ services  │  Publish ─────▶ │  Pulse Broker            │
   │           │                 │                          │
   │   ...     │                 │  adapters/grpc ────┐     │
   └───────────┘                 │        │           │     │
                                 │        ▼           │     │
    ┌────────────────────────┐   │  application/services     │
    │ pkg/api (wire types)   │   │  │        │        │      │
    └────────────────────────┘   │  │        ▼        │      │
                                 │  │  application/ports    │
                                 │  ▼        ▼        ▼     │
                                 │ infrastructure          │
                                 │  storage (data plane)   │
                                 │  storage (meta plane)   │
                                 └──────────────────────────┘
```

The broker is the only moving part in Phase 1: it accepts publishes, appends
them to a durable log, serves ordered streams to consumers, and tracks consumer
positions. Every future component (a second broker, a raft group, a metrics
exporter) is an additional adapter or an additional infrastructure
implementation: never a change to the domain.

## 2. Clean architecture

Pulse uses four layers with a strict dependency rule: dependencies point inward,
and the composition root wires them together.

```
        ┌───────────────────────────────┐
        │  domain                       │  stdlib only
        │  (entities, values, rules)    │
        └──────────────▲────────────────┘
                       │
        ┌──────────────┴────────────────┐
        │  application                  │  depends on domain + ports
        │  (ports, use cases, services) │
        └──────────────▲────────────────┘
                       │
   ┌───────────────────┴────────────────────┐
   │  adapters          │  infrastructure    │
   │  grpc, cli, ...    │  storage, config,  │
   │  (external-facing) │  logging, network  │
   └───────────────────┴────────────────────┘
        both depend on application ports + domain; never on each other

   cmd/pulse-server, internal/server  → composition root
```

Rules enforced by review (and by `go list`/import analysis):

1. `domain` imports only the standard library and `github.com/oklog/ulid/v2`.
2. `application` imports `domain` and `application/ports` only.
3. `adapters` and `infrastructure` import `application` and `domain`; neither
   imports the other.
4. Only `internal/server` and `cmd/*` import both adapters and infrastructure.
5. No package imports a sibling at the same layer to reach a lower layer it is
   not allowed to depend on.

The rationale: the domain expresses *what* the broker is; the application
expresses *what it does*; infrastructure expresses *how*; adapters express
*how it is reached*. Replacing Pebble with an internal-topic metadata store,
or zap with log/slog, is a composition change, not a code change.

## 3. Domain model

Entity / value objects (all in `internal/domain`):

| Concept | Package | Responsibility |
|---|---|---|
| `Message` | `message` | User-facing event model (see §7). |
| `Record` | `message` | A `Message` bound to a log position (offset + timestamp). |
| `RecordBatch` | `message` | An ordered group of records written atomically. |
| `Topic`, `TopicName`, `TopicConfig` | `topic` | Named event stream with validation and durable limits. |
| `Partition`, `PartitionID` | `partition` | Partition identity plus its lifecycle state. |
| `Offset` | `offset` | Immutable log position. |
| `Broker`, `BrokerID`, `NodeID`, `ClusterID`, `BrokerState` | `broker` | Broker identity and lifecycle. |
| `ConsumerID`, `Subscription` | `consumer` | Consumer identity and position intent. |
| `ProducerID` | `producer` | Producer identity (future idempotency key). |
| `RetentionPolicy` | `retention` | Durable time/size retention limits. |
| `Log` | `storage` | **Port** the partition log must satisfy. |
| `Clock` | `timeutil` | **Port** making time injectable. |

The domain owns invariants: topic-name syntax, partition bounds, offset
monotonicity, valid broker-state transitions, and message structural validity.
It knows nothing about segments, files, gRPC, or Pebble.

## 4. Application layer

The application layer contains ports and services.

**Ports (`application/ports`)**: the seams between the application and the
world:

- `MetadataStore`: durable broker state (topics, partition metadata, consumer
  cursors, cluster/broker identity). Implementations: `PebbleMetadataStore`,
  `InMemoryMetadataStore` (tests).
- `LogFactory`: creates/opens/deletes the persistent log for a partition.
- `Clock`: injectable time.
- `Logger`: structured logging.
- `MetricsRecorder`: observability hook (Noop in Phase 1).

**Services (`application/services`)**: orchestration and business rules:

- `Broker`: facade, lifecycle state machine, identity, startup recovery and
  graceful shutdown, BrokerInfo.
- `TopicManager`: create/delete/list topics across the metadata store and the
  log factory.
- `Publisher`: validates, assigns offsets/timestamps/event IDs, appends batches.
- `Subscriber`: ordered streams from a cursor or explicit offset, cursor
  tracking on Ack.
- `LogRegistry`: in-memory map of open logs, the sole owner of the topic→log
  lifecycle.

## 5. Data plane vs. metadata plane

Two storage concerns are deliberately separated:

- **Data plane** (event data) lives exclusively in the append-only segment log
  (`infrastructure/storage/engine`). It is immutable, checksummed, and
  append-ordered.
- **Metadata plane** (broker state) lives in the metadata store
  (`infrastructure/storage/metadata`): topic definitions, partition metadata,
  consumer cursors, cluster and broker identity.

They evolve independently. The metadata store never holds event payloads; the
log engine never holds broker state.

### Why Pebble for the metadata plane

- Mature and production-proven (backing CockroachDB and many others).
- Pure Go with no CGO, keeping the build and cross-compilation simple.
- Excellent point and range lookup performance for small, hot records.
- Simplifies early development while the log engine stabilizes.

Pebble is an **implementation detail**. The application depends only on the
`MetadataStore` port, so a future `InternalTopicMetadataStore` (log-compacted
internal topics, Kafka-style `__consumer_offsets`) can replace it by changing
dependency injection, not broker, application, or API code. The metadata
schema is deliberately key-namespaced to make that migration mechanical.

## 6. Cluster identity and broker lifecycle

Even though Phase 1 is a single node, the identity model is fully specified.

- `ClusterID`: identifies the cluster (a ULID, generated once, persisted).
- `BrokerID`: identifies this broker within the cluster (a ULID, generated on
  first start, persisted).
- `NodeID`: identifies the physical node; equal to `BrokerID` for single-node
  deployments, diverges when multiple brokers share a node (future).

All three are exposed through `BrokerInfo` today so clients and tooling observe
stable identity from the first deployment. Future raft-based replication
extends `Broker`/`BrokerState` with leader/follower roles instead of inventing
new concepts.

`BrokerState` is an explicit lifecycle:

```
Starting → Recovering → Running → Draining → Stopping → Stopped
```

- `Starting`: config loaded, metadata store opening.
- `Recovering`: logs opened, torn writes truncated, offsets restored.
- `Running`: serving requests.
- `Draining`: new work rejected, in-flight work drained (gRPC GracefulStop).
- `Stopping`: logs synced and closed.
- `Stopped`: resources released, process may exit.

Future phases map onto this: leader transfer runs during `Draining`, maintenance
mode is a sub-state of `Running`, and raft membership changes are valid only in
`Running`.

## 7. Event model

A `Message` carries the full event model. Fields are validated for structure;
semantics are left to producers and consumers.

| Field | Type | Broker behavior |
|---|---|---|
| `event_id` | ULID | Assigned at publish if absent; validated if present. |
| `key` | string | Opaque routing key (partitioning in a future phase). |
| `payload` | bytes | Opaque body; size-bounded. |
| `headers` | map | Passed through verbatim. |
| `content_type` | string | Passed through. |
| `correlation_id` / `trace_id` | string | Passed through. |
| `retry_count` | int32 | Passed through; redelivery increments later. |
| `ttl_ms` | int64 | Passed through; expiry enforcement later. |
| `priority` | int32 | Passed through; ordering semantics later. |
| `schema_version` | int32 | Passed through; registry validation later. |

**ULID for `event_id`**: lexicographically sortable, timestamp-ordered,
human-friendly, and locality-friendly for indexes, chosen over UUIDv4. See
`docs/Storage.md` for the format rationale.

## 8. Error model

Typed, sentinel errors live in the domain package that owns the failing
invariant (`topic.ErrAlreadyExists`, `offset.ErrOutOfRange`,
`message.ErrInvalid`, ...). The gRPC adapter maps domain errors to canonical
gRPC codes; nothing above the domain sees transport-specific errors.

Storage distinguishes **recoverable** corruption (torn write at the log tail,
truncated and logged during recovery) from **fatal** conditions (unreadable
metadata, unsupported format, broker refuses to start).

## 9. Extension points

| Future feature | Extends |
|---|---|
| Multi-partition topics | `PublishRequest.partition`, `SubscribeRequest.partition` already typed; only validation changes |
| Consumer groups, rebalancing | `consumer`, cursor schema, `Ack` becomes group commit |
| Replication, raft (per-partition) | `broker` identity/state, partition metadata |
| Dead-letter queues | internal topic namespace (`__` prefix reserved) |
| Compression, batching | reserved flags in the storage batch format |
| Exactly-once delivery | reserved producer id/sequence fields |
| Authentication / authorization | `adapters/grpc` interceptors; `ports.Authentication` |
| Metrics / tracing | `ports.MetricsRecorder` + `adapters/grpc` interceptors |
| Public SDK | `pkg/` additions (see Repository.md) |
| Log-compacted metadata | `MetadataStore` alternate implementation |

The invariant for future work: **a new phase adds a new adapter or a new
infrastructure implementation, never a new layer or a domain rewrite.**
