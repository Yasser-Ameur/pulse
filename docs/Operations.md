# Operations

Running Pulse in production: the shutdown sequence, Kubernetes probes, metrics,
logging, keepalive, limits, the errors a client sees, and a readiness
checklist. Every claim here names the code that implements it.

## Signals and the shutdown sequence

`server.Run` (`internal/server/server.go`) listens for `os.Interrupt` and
`SIGTERM`. On the first signal it runs this sequence, in order:

1. `recorder.SetUp(false)` sets the `pulse_up` metric to `0` immediately.
2. A `shutdownCtx` bounded by `shutdown-grace` (default `10s`) is created. A
   goroutine watches for a **second** signal and cancels it early, so a second
   Ctrl-C forces the stop instead of waiting out the grace window.
3. `app.Drain()` (`internal/application/services/broker.go`) moves the broker
   to `Draining`: new publish/subscribe/ack calls start returning
   `broker.ErrDraining`, and every live `Subscribe` stream is canceled
   immediately via its `drainCtx`, so those streams do not hold up the next
   step.
4. `transport.GracefulStop(shutdownCtx)`
   (`internal/infrastructure/grpc/server.go`) flips the gRPC health service to
   `NOT_SERVING`, then calls `grpc.Server.GracefulStop()` to drain in-flight
   unary RPCs. If `shutdownCtx` expires first, it calls `Stop()` to
   force-close them.
5. `app.Shutdown(shutdownCtx)` drains again (idempotent), syncs and closes
   every open log, and closes the metadata store.
6. The monitor HTTP listener is shut down **last**, after the storage flush,
   so `/healthz` and `/readyz` keep answering through drain and flush instead
   of refusing the connection early.

Draining before `GracefulStop` means followers are already gone by the time
`GracefulStop` starts, so it only has to wait out in-flight unary calls, not
the whole grace window on top of them.

## Kubernetes probes

Point the container's probes at the monitor listener (default port `9091`):

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 9091
readinessProbe:
  httpGet:
    path: /readyz
    port: 9091
```

`/healthz` answers `200` for as long as the process should keep running,
including while `Draining`, so Kubernetes does not kill a pod that is mid
shutdown. `/readyz` answers `200` only while the broker is `Running`, so a
draining or still-recovering broker is taken out of the Service's endpoint
list without being restarted (`internal/infrastructure/monitor/monitor.go`).
The same distinction backs the gRPC health service
(`grpc.health.v1.Health`, service names `""`, `pulse.v1.Broker`,
`pulse.v1.PubSub`), so `grpc_health_probe` works as an alternative probe.

## The healthcheck subcommand and Docker

`pulse-server healthcheck --config <file>` (or `--monitor-addr`,
overriding whatever `--config` resolves) probes `/readyz` and exits
non-zero on anything but a healthy response (`cmd/pulse-server/healthcheck.go`).
It exists so a container runtime can shell out to a single binary instead of
needing `curl` in the image. The `Dockerfile`'s own `HEALTHCHECK` uses it:

```
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/pulse-server", "healthcheck", "--config", "/app/config.yaml"]
```

`examples/docker-compose.yaml` wires up the same command as the compose
`healthcheck` entry, plus a Prometheus service scraping the broker's monitor
listener with the scrape config in `examples/prometheus.yml`:

```bash
docker compose -f examples/docker-compose.yaml up
```

## Prometheus

Scrape the monitor listener's `/metrics`:

```yaml
scrape_configs:
  - job_name: pulse
    static_configs:
      - targets: ["pulse-broker:9091"]
```

Metrics registered in `internal/infrastructure/metrics/prometheus.go`, plus
the Go and process collectors:

| Metric | Type | Meaning |
|---|---|---|
| `pulse_up` | gauge | `1` while running, `0` from the start of shutdown. |
| `pulse_broker_info` | gauge | Always `1`, labeled `version`. |
| `pulse_publish_records_total` | counter | Records published. |
| `pulse_publish_bytes_total` | counter | Payload bytes published. |
| `pulse_consume_records_total` | counter | Records delivered to consumers. |
| `pulse_consume_bytes_total` | counter | Payload bytes delivered to consumers. |
| `pulse_publish_latency_seconds` | histogram | Publish handler latency. |
| `pulse_consume_latency_seconds` | histogram | Subscribe read loop latency. |
| `pulse_storage_bytes_written_total` | counter | Bytes durably written. |
| `pulse_storage_bytes_read_total` | counter | Bytes read from storage. |

`GET /varz` (see [Configuration.md](Configuration.md)) carries the same
broker-wide counters as plain fields for a quick look without a Prometheus
query: `connections`, `subscriptions`, `published_records`,
`published_bytes`, `delivered_records`, and `delivered_bytes`
(`services.Broker.Stats`, `internal/application/services/broker.go`).

## Logging

`log-level` (`debug`, `info`, `warn`, `error`) and `development` (human vs.
JSON output) are the two knobs, both resolved by `Load` and consumed by
`logging.NewZapLogger` (`internal/server/server.go`). Every completed RPC is
logged once at `debug` with method, gRPC code, and duration
(`internal/infrastructure/grpc/server.go`); a handler panic is recovered,
logged at `error` with a stack trace, and returned to the client as
`codes.Internal` without leaking that trace over the wire.

## Keepalive

The gRPC server (`internal/infrastructure/grpc/server.go`) sets:

- `MaxConnectionIdle: 5m`: an idle connection is closed after five minutes.
- `Time: 2m`, `Timeout: 20s`: a keepalive ping is sent every two minutes and
  the connection is closed if the pong does not arrive within 20 seconds.
- Enforcement policy `MinTime: 30s`, `PermitWithoutStream: true`: a client
  sending pings more often than every 30 seconds is disconnected, even with
  no active RPC.

## Limits

- **Max batch size**: `message.MaxBatchRecords = 10000`
  (`internal/domain/message/message.go`): a single publish batch is rejected
  past this many records.
- **Message structural limits** (`internal/domain/message/message.go`): 64
  headers per message, 512-byte header keys, 8192-byte header values, a
  4096-byte message key. Payload size is bounded per topic by
  `TopicConfig.MaxMessageBytes`.
- **Transport frame size**: `max-recv-msg-size` / `max-send-msg-size`, default
  64 MiB each, validated up to 256 MiB (see [Configuration.md](Configuration.md)).
- **Subscribe read caps**: `subscribe.read-limit` (default 512 records) and
  `subscribe.read-max-bytes` (default 1 MiB) bound a single read from the log.

## Multi-partition topics

A topic now takes 1 to `topic.MaxPartitions` (256) partitions at creation
(`TopicManager.CreateTopic`, `internal/application/services/topic_manager.go`).
Ordering stays per-partition, not per-topic: each partition has its own
append lock and its own contiguous offsets, and `Ack` stores a separate
cursor per `(consumer, topic, partition)`
(`internal/application/services/subscriber.go`). Route related messages to
the same partition yourself; the broker does not hash keys for you (see
[Client.md](Client.md) for `PartitionForKey`).

## Errors a client sees

Mapped in `internal/adapters/grpc/errors.go`:

| Situation | gRPC code |
|---|---|
| Broker is `Draining` or `Stopping` | `Unavailable` ("broker draining") |
| Broker is not yet `Running` | `Unavailable` ("broker not running") |
| Reserved topic name (`__` prefix) | `InvalidArgument` |
| Oversized batch, payload, key, or headers | `InvalidArgument` |
| Topic or partition not found | `NotFound` |
| Topic already exists | `AlreadyExists` |
| Requested offset outside the log's range | `OutOfRange` |
| Client canceled the request | `Canceled` |
| Anything unclassified | `Internal` |

`pkg/client` maps `NotFound`, `AlreadyExists`, `InvalidArgument`, and
`Unavailable` to exported sentinel errors (see [Client.md](Client.md)) and
retries `Unavailable` automatically on `Publish` and, with `Follow: true`, on
`Subscribe`.

## Production readiness checklist

Each item points at the config key or command that satisfies it, in the style
of a self-hoster's day-one checklist.

- **Liveness and readiness probes**: `GET /healthz`, `GET /readyz` on
  `monitor-addr` (default `9091`).
- **Metrics**: `GET /metrics` on `monitor-addr`, scraped by Prometheus.
- **Graceful shutdown under load**: `shutdown-grace` sized to your longest
  expected in-flight RPC; the broker drains followers before it starts the
  grace timer, so the timer only has to cover unary work.
- **Encrypted transport**: `tls.cert-file` + `tls.key-file`; add
  `tls.client-ca-file` for mTLS. See [Configuration.md](Configuration.md).
- **Client authentication**: `auth.tokens` or `PULSE_AUTH_TOKENS`. Off by
  default, and the broker warns at startup while it is off. See
  [Configuration.md](Configuration.md) and [Guarantees.md](Guarantees.md) for
  what a valid token can and cannot do.
- **Durability guarantee matches your risk tolerance**: `storage.sync-mode:
  every-write` (default) fsyncs every append; `interval` trades that for
  throughput at the cost of up to `storage.sync-interval` of data on a crash.
- **Retention configured per topic**: `storage.retention-interval` runs the
  sweeper; each topic's own `MaxAge`/`MaxBytes` decide what it trims.
- **Batch and message limits sized to your workload**: `message.MaxBatchRecords`
  (10000, not configurable) and `max-recv-msg-size` / `max-send-msg-size`
  bound what a single publish can carry.
- **Structured logs at the right level**: `log-level: info` in production,
  `debug` only while diagnosing, since every RPC is logged at `debug`.
- **Data directory on durable, backed-up storage**: `data-dir`; Pulse is
  single-node with no replication, so losing the data directory loses the
  data (see [Guarantees.md](Guarantees.md)).
- **Version pinned and visible**: `pulse_broker_info{version=...}` and
  `GET /varz` report the running build; images are stamped from the release
  tag (`Dockerfile`, `.github/workflows/release.yml`).
- **Tests pass a coverage floor**: `make coverage-check` (`COVERAGE_FLOOR`,
  default 50, in the `Makefile`) fails CI when total coverage drops below it,
  wired as the "Coverage floor" step in `.github/workflows/ci.yml`.
- **Not yet provided**: per-topic authorization. A valid token can publish,
  subscribe, and administer every topic; there is no per-token scoping. Do
  not hand a token to a client you do not trust with the whole broker.
