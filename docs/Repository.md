# Repository Layout

Pulse is organized as a monorepo containing two binaries (`pulse-server`, `pulse-cli`), a public wire-API package, and the strictly layered internal implementation.

```
pulse/
├── api/                          # Protocol contract source of truth
│   └── proto/pulse/v1/           #   model.proto, broker.proto, pubsub.proto
├── pkg/                          # Public, stable Go packages (exported API)
│   ├── api/pulse/v1/pulsepb/     #   Generated protobuf + gRPC stubs
│   └── client/                   #   Public Go client (Dial, Publish, Subscribe, Ack)
├── cmd/                          # Executable entrypoints (thin)
│   ├── pulse-server/             #   Broker process
│   └── pulse-cli/                #   Command-line client
├── internal/                     # Implementation (not importable outside the module)
│   ├── domain/                   # Pure domain model. Stdlib only.
│   │   ├── message/              #   Message, Record, RecordBatch, Headers, EventID (ULID)
│   │   ├── topic/                #   Topic, TopicName, TopicConfig, validation
│   │   ├── partition/            #   Partition, PartitionID, PartitionState
│   │   ├── offset/               #   Offset semantics
│   │   ├── broker/               #   Broker, BrokerID, NodeID, ClusterID, BrokerState
│   │   ├── consumer/             #   ConsumerID, Subscription
│   │   ├── producer/             #   ProducerID
│   │   ├── retention/            #   RetentionPolicy
│   │   ├── storage/              #   Log port interface
│   │   └── timeutil/             #   Clock interface
│   ├── application/              # Use cases + ports. Depends on domain only.
│   │   ├── ports/                #   MetadataStore, LogFactory, Clock, Logger, MetricsRecorder
│   │   └── services/             #   Broker, TopicManager, Publisher, Subscriber, LogRegistry
│   ├── adapters/                 # External-facing adapters. Depends on application + domain.
│   │   ├── grpc/                 #   gRPC service handlers + error mapping
│   │   └── cli/                  #   Cobra command tree
│   ├── infrastructure/           # Pluggable implementations of ports. Depends on ports + domain.
│   │   ├── config/               #   YAML + environment configuration and validation
│   │   ├── timeutil/             #   SystemClock
│   │   ├── logging/              #   Zap adapter behind ports.Logger
│   │   ├── metrics/              #   Noop recorder (future Prometheus/OTel adapters)
│   │   ├── storage/
│   │   │   ├── filesystem/       #   Path layout, file I/O, durability helpers
│   │   │   ├── engine/           #   Append-only log engine (data plane)
│   │   │   │   ├── checksum/     #     CRC32C
│   │   │   │   ├── codec/        #     Record batch binary codec
│   │   │   │   ├── index/        #     Sparse offset index
│   │   │   │   ├── segment/      #     Immutable segment files
│   │   │   │   ├── recovery/     #     Crash recovery and truncation
│   │   │   │   ├── snapshot/     #     Durable recovery checkpoints
│   │   │   │   └── log/          #     Log coordinator
│   │   │   └── metadata/         #   Metadata plane: PebbleMetadataStore, InMemoryMetadataStore
│   │   └── grpc/                 #   gRPC server plumbing (transport, interceptors, TLS)
│   ├── server/                   # Composition root (the only place layers meet)
│   └── testutil/                 # In-process broker harness for integration tests
├── tests/integration/            # End-to-end tests against a real in-process broker
├── examples/                     # Runnable example programs
├── bench/                        # Separate module: benchmark harness vs. other brokers
├── docs/                         # Architecture and design documentation
└── .github/workflows/ci.yml      # CI pipeline
```

## Why this layout

- **`api/` vs `pkg/`**: the `.proto` files are the single source of truth for the
  wire contract; `pkg/api/.../pulsepb` holds the generated, importable Go types.
  Keeping them separate means the protocol can be reviewed as plain text and the
  generated artifacts are plainly regenerable from the `.proto` sources.
- **`pkg/` is the only public surface**: `pulsepb` is exported so that external
  SDKs (planned: Python, Java) and integrations can reuse the canonical wire
  types, and `pkg/client` is the first-class Go SDK built on it (see
  [Client.md](Client.md)). Everything else is under `internal/`, which Go
  enforces as module-private; `pulse-cli` and the integration tests now dial
  through `pkg/client` too, so the broker has no remaining internal gRPC
  client.
- **`cmd/` stays thin**: binaries do nothing but call the composition root
  (`internal/server`) or the CLI adapter. All logic lives in testable packages.
- **Strict layering of `internal/`**: `domain → application → adapters` and
  `infrastructure` are independent implementations of `application/ports`. The
  dependency graph is acyclic (see Architecture.md).
- **`adapters/` vs `infrastructure/`**: adapters translate between the outside
  world and the application (gRPC handlers, CLI, future REST/SDK); infrastructure
  provides concrete implementations of ports (Pebble, filesystem, config,
  logging, networking). Transport concerns stay independent of persistence
  concerns.
- **`tests/integration/` is a separate module package** so the test harness can
  import `internal/testutil` without polluting library packages.

## Rules of the repository

1. New code goes under `internal/` unless it is part of the public API.
2. No package outside `cmd/` and `internal/server/` may import `adapters` and
   `infrastructure` together.
3. `domain` never imports anything outside the standard library and `oklog/ulid`.
4. Every package has a package comment on the file that best represents it
   (a `doc.go` only where no single file is the obvious home, as in
   `adapters/grpc`, `domain/timeutil`, and `infrastructure/storage/metadata`);
   every exported symbol is documented.
5. Generated files (`pkg/api/**`) are committed but never hand-edited.
