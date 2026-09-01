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

## Guarantees

Pulse delivers each record **at-least-once**, ordered **totally within a
partition**, and deduplicated **nowhere** — **consumers must be idempotent**. A
consumer that crashes between processing a record and acknowledging it will see
that record again on resume.

A stored cursor is the **next** offset to consume, not the last one consumed:
after processing offset `N`, acknowledge `N+1`.

Publishes are acknowledged after fsync under the default `sync-mode:
every-write`, and before fsync under `sync-mode: interval` (losing up to
`sync-interval` on a machine crash). Independently of the mode, **a record can
be delivered before it is durable** — there is no high watermark in the log.

Not provided, deliberately: exactly-once delivery, clustering, replication,
consumer groups, and authentication. TLS is provided (see Highlights below).

Read [docs/Guarantees.md](docs/Guarantees.md) before writing a consumer — it
states each of these precisely, names the code that implements them, and lists
what Pulse does not give you.

## Highlights

- **Durable by default**: under the default `sync-mode: every-write`, publishes
  are acknowledged only after fsync and acknowledged messages are never lost;
  torn writes are recovered by CRC-validated truncation. `sync-mode: interval`
  trades that for throughput — see [Guarantees.md](docs/Guarantees.md) §2.
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
  and one documented shutdown sequence: drain followers, then `GracefulStop`,
  then flush and close storage, with the monitor listener staying up
  throughout (see [Operations.md](docs/Operations.md)).
- **Production probes and metrics**: a separate monitor listener (default
  `127.0.0.1:9091`) serves `/healthz` (liveness), `/readyz` (readiness), `/varz`
  (JSON status), and `/metrics` (Prometheus), so Kubernetes and Prometheus need
  no data-plane access (see [Configuration.md](docs/Configuration.md)).
- **TLS and mTLS**: `tls.cert-file`/`tls.key-file` enable server TLS,
  `tls.client-ca-file` adds client certificate verification; the public Go
  client and `pulse-cli` (`--tls-ca`, `--tls-cert`, `--tls-key`,
  `--tls-skip-verify`) dial with the same credentials (see
  [Client.md](docs/Client.md)).
- **A public Go client**: `pkg/client` is a standalone, importable SDK with
  automatic retry on `Unavailable` and transparent `Subscribe` resume (see
  [Client.md](docs/Client.md)).

## Quickstart

Requires Go 1.26+.

```bash
# build both binaries
make build

# start a broker on 127.0.0.1:9090 (gRPC) and 127.0.0.1:9091 (health,
# readiness, metrics) with data in ./data
bin/pulse-server --config examples/config.yaml

# in another shell
bin/pulse-cli topics create orders
bin/pulse-cli publish orders --key user-42 --message '{"sku":"a1"}'
bin/pulse-cli subscribe orders --follow --consumer warehouse

# with TLS: bin/pulse-cli --tls-ca ca-cert.pem ...
```

Or drive it from Go with the public client, `pkg/client`:

```go
c, err := client.Dial("127.0.0.1:9090")
if err != nil {
    log.Fatal(err)
}
defer c.Close()

ctx := context.Background()
if _, err := c.Publish(ctx, "orders", 0, client.Message{
    Payload: []byte(`{"sku":"a1"}`),
}); err != nil {
    log.Fatal(err)
}

err = c.Subscribe(ctx, "orders", 0, client.SubscribeOptions{Consumer: "warehouse"},
    func(r client.Record) error {
        fmt.Println(r.Offset, string(r.Message.Payload))
        return nil
    })
```

See [examples/](examples/) for configs and runnable programs, and
[Client.md](docs/Client.md) for the full client reference.

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

- [Guarantees.md](docs/Guarantees.md) — delivery, durability, and non-goals
- [Repository.md](docs/Repository.md) — layout and rules
- [Architecture.md](docs/Architecture.md) — layers, domain model, extension points
- [Storage.md](docs/Storage.md) — log format, indexes, recovery, retention
- [Protocol.md](docs/Protocol.md) — gRPC contract and versioning
- [Concurrency.md](docs/Concurrency.md) — goroutines, locks, shutdown
- [Configuration.md](docs/Configuration.md): every config key, TLS setup, monitor endpoints
- [Operations.md](docs/Operations.md): production shutdown, probes, metrics, limits, readiness checklist
- [Client.md](docs/Client.md): the public Go client and CLI TLS flags
- [Roadmap.md](docs/Roadmap.md) — phase plan and extension points

## Development

```bash
make fmt      # gofmt + goimports
make lint     # golangci-lint
make test     # go test ./...
make test-race # go test -race ./...
make coverage # coverage report (coverage.out, coverage.html)
```

CI (GitHub Actions) checks formatting, vets and lints, runs unit and
integration tests, runs them again with the race detector, and builds both
binaries. It does not collect coverage; run `make coverage` locally for that.

## License

Apache License 2.0. See [LICENSE](LICENSE).
