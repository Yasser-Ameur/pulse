# Pulse

Pulse is a distributed event streaming platform: durable, ordered, replayable
delivery of events between independent services. It is the messaging backbone
of the FlowOS ecosystem and is designed to become a serious open-source
infrastructure project.

Pulse is inspired by Kafka, NATS JetStream, Redpanda, and Pulsar, but is none
of them. It is an independent implementation with clean architecture, durable
append-only storage, and a deliberate, documented path toward clustering.

## Status

**Phase 1 (core broker)** — single-node broker with a durable segment log,
topics, publish/subscribe with acknowledgements, a gRPC API, a CLI, and a full
test suite. See [docs/Roadmap.md](docs/Roadmap.md).

## Highlights

- **Durable by default**: publishes are acknowledged only after fsync; torn
  writes are recovered by CRC-validated truncation — acknowledged messages are
  never lost.
- **Append-only segment log** with sparse offset indexes, checksummed batches,
  and deterministic crash recovery.
- **Clean architecture**: strict layering (`domain → application → adapters` +
  `infrastructure`), ports for metadata storage, time, logging, and metrics —
  designed so replication, consumer groups, observability, and auth are new
  adapters, not rewrites.
- **Single dependency for local dev**: Go only. No Docker required to build,
  test, or run (`go test ./...`). Docker/Testcontainers is reserved for future
  cluster testing.
- **Deterministic**: injectable clock, typed errors, total per-partition order,
  and one documented shutdown sequence.

## Quickstart

Requires Go 1.26+.

```bash
# build both binaries
make build

# start a broker on 127.0.0.1:9090 with data in ./data
bin/pulse-server --config examples/config.yaml

# in another shell
bin/pulse-cli topic create orders
bin/pulse-cli publish orders --key user-42 --value '{"sku":"a1"}'
bin/pulse-cli consume orders --follow --consumer warehouse
```

See [examples/](examples/) for configs and runnable programs.

## Repository map

| Path | Purpose |
|---|---|
| `api/proto/pulse/v1/` | Wire contract (protobuf sources) |
| `pkg/api/pulse/v1/pulsepb/` | Generated Go wire types |
| `cmd/pulse-server`, `cmd/pulse-cli` | Binaries |
| `internal/domain` | Pure domain model |
| `internal/application` | Ports, use cases, services |
| `internal/adapters` | gRPC handlers and CLI |
| `internal/infrastructure` | Storage engine, metadata store, config, logging, networking |
| `internal/server` | Composition root |
| `tests/integration/` | End-to-end in-process tests |
| `docs/` | Design documentation |

## Documentation

- [Repository.md](docs/Repository.md) — layout and rules
- [Architecture.md](docs/Architecture.md) — layers, domain model, extension points
- [Storage.md](docs/Storage.md) — log format, indexes, recovery, retention
- [Protocol.md](docs/Protocol.md) — gRPC contract and versioning
- [Concurrency.md](docs/Concurrency.md) — goroutines, locks, shutdown
- [Roadmap.md](docs/Roadmap.md) — phase plan and extension points

## Development

```bash
make fmt      # gofmt + goimports
make lint     # golangci-lint
make test     # go test ./...
make test-race # go test -race ./...
make coverage # coverage report (coverage.out, coverage.html)
```

CI (GitHub Actions) runs formatting, linting, unit + integration tests with the
race detector, builds both binaries, and collects coverage.

## License

To be decided before first public release.
