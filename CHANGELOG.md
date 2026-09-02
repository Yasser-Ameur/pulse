# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2026-09-02

### Fixed

- The release workflow downloads only the build matrix archives, so the
  release job no longer fails on the Docker build record artifact that the
  container job uploads alongside them. v0.3.0 carries the module rename
  but no release assets because of this.

## [0.3.0] - 2026-09-02

### Changed

- The Go module path is now `github.com/Yasser-Ameur/pulse`, the repository's
  actual host, so `go get github.com/Yasser-Ameur/pulse` resolves without a
  `replace` directive. Every import, the bench module, the Makefile, the
  Dockerfile, the release workflow and the client docs moved with it.

## [0.2.0] - 2026-09-02

### Added

- Log compaction for `compact` topics: `Log.Compact` deduplicates sealed
  segments to the newest record per key, keeps keyless records, supports
  tombstones (a keyed record with a nil payload), and never renumbers
  offsets, via a crash-safe copy-and-swap rewrite.
- `storage.compaction-interval` (default `30s`), `storage.compaction-tombstone-retention`
  (default `24h`), and `storage.compaction-min-gain-ratio` (default `0.1`)
  configuration keys, each with a `PULSE_STORAGE_*` environment override.
- Version embedding: the build and the container image now stamp the
  broker's version from the git tag instead of reporting `dev`.
- A tag-triggered release workflow that cross-builds `pulse-server` and
  `pulse-cli` for linux, darwin, and windows, publishes checksummed
  archives as a GitHub release, and pushes the container image to
  `ghcr.io/yasser-ameur/pulse`.
- `govulncheck` as a CI step.
- The bench harness now runs as part of the CI gate.
- A monitor HTTP listener (`monitor-addr`, default `127.0.0.1:9091`) serving
  `/healthz` (liveness), `/readyz` (readiness), `/varz` (JSON status), and
  `/metrics` (Prometheus), independent of the gRPC data plane.
- Prometheus metrics: `pulse_up`, `pulse_broker_info`, publish/consume record
  and byte counters, publish/consume latency histograms, and storage
  read/write byte counters.
- TLS on the gRPC transport, with optional mTLS via a client CA file.
- Strict YAML config decoding (unknown keys are now an error) and a
  `PULSE_*` environment override for every configuration key.
- A public Go client, `pkg/client`, with automatic retry on `Unavailable` and
  transparent `Subscribe` resume when following a topic.
- `--tls-ca`, `--tls-cert`, `--tls-key`, and `--tls-skip-verify` flags on
  `pulse-cli`.
- A configurable publish batch size limit (`message.MaxBatchRecords`, 10000
  records).
- Token authentication on the gRPC transport (`auth.tokens`,
  `auth.token-file`, `PULSE_AUTH_TOKENS`, `PULSE_AUTH_TOKEN_FILE`), off by
  default with a startup warning while it is off; `client.WithToken`,
  `client.ErrUnauthenticated`, and `pulse-cli --token` / `PULSE_TOKEN`.
- Broker-wide counters in `GET /varz`: `connections`, `subscriptions`,
  `published_records`, `published_bytes`, `delivered_records`,
  `delivered_bytes`.
- `pulse-server healthcheck --config <file>` / `--monitor-addr`, used by the
  Dockerfile `HEALTHCHECK` and `examples/docker-compose.yaml`.
- Multi-partition topics: 1 to 256 partitions per topic, each with its own
  order and cursor; `client.PartitionForKey` and `pulse-cli publish --key`
  route by key on the caller's side.
- A coverage floor (`make coverage-check`) enforced in CI, and an SBOM plus
  build provenance attached to release artifacts.
- `make image` to build the container image, and
  `examples/docker-compose.yaml` running the broker alongside Prometheus
  (`examples/prometheus.yml`).

### Changed

- Shutdown now drains followers (`Broker.Drain`) before `GracefulStop`, so
  the grace window only has to cover in-flight unary RPCs, and forces a stop
  on a second shutdown signal.
- Liveness and readiness are now reported separately: `/healthz` stays up
  through drain, `/readyz` goes unready as soon as draining starts.
- The monitor listener now stays up through the storage flush and is the
  last listener to shut down.
- `pulse-cli` and the integration tests now use `pkg/client` instead of the
  internal gRPC client, which is deleted.
- Client retry backoff (`Publish` and `Subscribe`) now uses full jitter
  instead of a fixed delay, to avoid a thundering herd of clients retrying in
  lockstep.

### Fixed

- `subscribe --forever` no longer dies after 30 seconds; only unary CLI
  commands are bounded by the unary timeout.
- A reserved topic name now maps to `InvalidArgument` instead of an
  unclassified error.
- Live subscribers are now canceled on shutdown instead of being left to
  time out.

## [0.1.0-phase1] - 2026-08-05

Phase 1: the core broker, shipped as a single-node service with a durable
append-only log.

### Added

- Topics: create, delete, and list, with persisted metadata.
- Publish (batch to offsets), Subscribe (streaming, follow and replay),
  and Ack (cursor) over a gRPC API.
- A CLI for administering the broker.
- In-process integration tests and CI.
- Full architecture and storage documentation.
