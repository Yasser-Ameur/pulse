# Configuration

Pulse is configured from three sources, applied in increasing precedence:
built-in defaults, an optional YAML file, then `PULSE_*` environment
variables. The final result is validated before the broker starts. All of
this is implemented in `internal/infrastructure/config/config.go`.

## Reference

Every YAML key has a matching `PULSE_<UPPER_SNAKE>` environment override,
named after its YAML path with dots replaced by underscores and hyphens
dropped. Four names predate that rule and are kept for compatibility:
`PULSE_LISTEN_ADDR`, `PULSE_DATA_DIR`, `PULSE_LOG_LEVEL`, `PULSE_SYNC_MODE`
(the last still works alongside the rule-conforming `PULSE_STORAGE_SYNC_MODE`,
which wins if both are set).

| YAML path | Env variable | Type | Default | Meaning | Validation |
|---|---|---|---|---|---|
| `listen-addr` | `PULSE_LISTEN_ADDR` | string | `127.0.0.1:9090` | gRPC listen address. | Must not be empty. |
| `data-dir` | `PULSE_DATA_DIR` | string | `data` | Root of the data and metadata plane directories. | Must not be empty. |
| `max-recv-msg-size` | `PULSE_MAX_RECV_MSG_SIZE` | int (bytes) | `67108864` (64 MiB) | Bounds a single received gRPC frame. | `0 < n <= 256 MiB`. |
| `max-send-msg-size` | `PULSE_MAX_SEND_MSG_SIZE` | int (bytes) | `67108864` (64 MiB) | Bounds a single sent gRPC frame. | `0 < n <= 256 MiB`. |
| `log-level` | `PULSE_LOG_LEVEL` | string | `info` | zap log level. | Must be `debug`, `info`, `warn`, or `error`. |
| `development` | `PULSE_DEVELOPMENT` | bool | `false` | Human-readable (vs. JSON) log output. | Any value `strconv.ParseBool` accepts. |
| `shutdown-grace` | `PULSE_SHUTDOWN_GRACE` | duration | `10s` | Grace window for draining in-flight RPCs before a forced stop. | Must not be negative. |
| `monitor-addr` | `PULSE_MONITOR_ADDR` | string | `127.0.0.1:9091` | Address the monitor HTTP listener binds to. Empty disables it. | None; empty is a valid, meaningful value. |
| `storage.segment-max-bytes` | `PULSE_STORAGE_SEGMENT_MAX_BYTES` | int64 (bytes) | `536870912` (512 MiB) | Segment rotates past this size. | `0 < n <= 2 GiB`. |
| `storage.index-interval-bytes` | `PULSE_STORAGE_INDEX_INTERVAL_BYTES` | int64 (bytes) | `4096` | Sparse index entry added per this many bytes. | Must be positive. |
| `storage.sync-mode` | `PULSE_SYNC_MODE`, `PULSE_STORAGE_SYNC_MODE` | string | `every-write` | `every-write` fsyncs every append; `interval` fsyncs periodically. | Must be `every-write` or `interval`. |
| `storage.sync-interval` | `PULSE_STORAGE_SYNC_INTERVAL` | duration | `100ms` | fsync period for `interval` mode. | Must be positive. |
| `storage.retention-interval` | `PULSE_STORAGE_RETENTION_INTERVAL` | duration | `30s` | How often the retention sweeper runs; `0` disables it. | Must not be negative. |
| `subscribe.read-limit` | `PULSE_SUBSCRIBE_READ_LIMIT` | int | `512` | Max records returned per subscribe read. | Must be positive. |
| `subscribe.read-max-bytes` | `PULSE_SUBSCRIBE_READ_MAX_BYTES` | int (bytes) | `1048576` (1 MiB) | Max payload bytes returned per subscribe read. | Must be positive. |
| `tls.cert-file` | `PULSE_TLS_CERT_FILE` | string | `""` | PEM server certificate path. Empty (with `key-file` empty) serves plaintext. | Must be set together with `key-file`. |
| `tls.key-file` | `PULSE_TLS_KEY_FILE` | string | `""` | PEM server private key path. | Must be set together with `cert-file`. |
| `tls.client-ca-file` | `PULSE_TLS_CLIENT_CA_FILE` | string | `""` | PEM CA bundle; when set, requires and verifies client certificates (mTLS). | Requires `cert-file` and `key-file` to be set. |

A duration value is a Go duration string such as `100ms` or `10s`, both in
YAML and in the environment; it is parsed with `time.ParseDuration`.

## Precedence and strict parsing

`Load(path)` in `internal/infrastructure/config/config.go` applies the three
sources in order:

1. `Default()` fills in every field above.
2. If `path` is non-empty, the YAML file at `path` is decoded on top of the
   defaults with `KnownFields(true)`. An unknown key in the file is a decode
   error, not a silently ignored typo.
3. `applyEnv()` overrides individual fields from `PULSE_*` environment
   variables, collecting every malformed value (a bad bool, int, or duration)
   into a single joined error naming the offending variable, rather than
   stopping at the first one.
4. `Validate()` runs last, checking every field in the table above and joining
   every violation into one error.

`Load` returns as soon as any of these three stages fails; a broker never
starts on a configuration it could not fully resolve and validate.

## TLS and mTLS setup

TLS is off by default (plaintext gRPC). Setting `tls.cert-file` and
`tls.key-file` enables server-side TLS; additionally setting
`tls.client-ca-file` requires and verifies a client certificate (mTLS). The
server resolves this into a `*tls.Config` in `buildTLSConfig`
(`internal/server/server.go`) before any listener opens, with
`MinVersion: tls.VersionTLS12`, and passes it to the gRPC transport via
`grpc.Creds(credentials.NewTLS(tlsConfig))`
(`internal/infrastructure/grpc/server.go`). A malformed certificate, key, or
CA bundle fails startup before the monitor or gRPC listener is created.

Server-only TLS, generated with `openssl`:

```bash
openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \
  -keyout server-key.pem -out server-cert.pem \
  -subj "/CN=pulse-broker" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

```yaml
tls:
  cert-file: "server-cert.pem"
  key-file: "server-key.pem"
```

mTLS, with a private CA that signs both the server and client certificates:

```bash
# CA
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
  -keyout ca-key.pem -out ca-cert.pem -subj "/CN=pulse-ca"

# server cert, signed by the CA
openssl req -newkey rsa:4096 -nodes -keyout server-key.pem -out server.csr \
  -subj "/CN=pulse-broker" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -days 365 -out server-cert.pem \
  -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")

# client cert, signed by the same CA
openssl req -newkey rsa:4096 -nodes -keyout client-key.pem -out client.csr \
  -subj "/CN=pulse-client"
openssl x509 -req -in client.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -days 365 -out client-cert.pem
```

```yaml
tls:
  cert-file: "server-cert.pem"
  key-file: "server-key.pem"
  client-ca-file: "ca-cert.pem"
```

The client side (`pkg/client.WithTLS` and the `pulse-cli` `--tls-*` flags) is
covered in [Client.md](Client.md).

## Monitor endpoints

`monitor-addr` (default `127.0.0.1:9091`) binds a plain HTTP listener served
by `internal/infrastructure/monitor.New`, independent of the gRPC listener so
a probe or scrape never competes with the data plane. Setting `monitor-addr`
to `""` disables it. Full operational meaning of each endpoint is in
[Operations.md](Operations.md); this is the JSON shape.

`GET /healthz`: liveness. `200` while the broker is anything but `Stopped`;
`503` once it has fully stopped.

```json
{"status":"ok"}
```

`GET /readyz`: readiness. `200` only while the broker is `Running`; `503`
in every other state, with `status` naming it.

```json
{"status":"draining"}
```

`GET /varz`: a JSON status dump.

```json
{
  "version": "0.2.0",
  "broker_id": "01J...",
  "cluster_id": "01J...",
  "state": "running",
  "uptime_seconds": 128.4,
  "started_at": "2026-09-02T10:00:00Z",
  "topics": [
    {"name": "orders", "partitions": [{"id": 0, "end_offset": 42}]}
  ],
  "go_version": "go1.26",
  "num_goroutine": 12
}
```

`partitions[].end_offset` is the next offset the partition will assign. There
is no start offset: the storage `Log` port exposes only the log end
(`NextOffset`), not a trimmed low-water mark
(`internal/application/services/monitor_view.go`).

`GET /metrics`: Prometheus exposition format, served by
`promhttp.HandlerFor` over the same registry the broker's own counters are
registered on; see [Operations.md](Operations.md) for the metric names.
