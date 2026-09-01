# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
