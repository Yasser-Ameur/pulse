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
