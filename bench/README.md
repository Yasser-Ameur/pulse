# Pulse bench

`bench/` is a standalone Go module that drives Pulse and NATS JetStream
through the same workload driver (`harness.go`), so the two systems are
started, published to, and crashed by identical code.

## Build

From the repository root, build both binaries the harness needs:

```
go build -o bin/pulse-server ./cmd/pulse-server
```

and install `nats-server`:

```
go install github.com/nats-io/nats-server/v2@latest
```

Then, from `bench/`:

```
go build -o bin/bench .
```

## Run

Publish workload, both targets:

```
go run . -target=pulse,jetstream -mode=publish -n=20000 -conc=1,8,32 \
  -warmup=1000 -pulse-bin=../bin/pulse-server -sync=true
```

Recovery workload, one target:

```
go run . -target=pulse -mode=recovery -pulse-bin=../bin/pulse-server
```

Flags: `-target` (comma-separated `pulse`/`jetstream`), `-mode`
(`publish` or `recovery`), `-n` (measured publishes), `-size` (payload
bytes), `-conc` (comma-separated concurrency levels), `-warmup` (publishes
discarded before measurement), `-sync` (fsync every write; `false` runs the
relaxed control arm), `-dir` (scratch directory for broker data), `-out`
(write results as JSON instead of, or in addition to, the printed table).

## Result fields

Publish mode reports one `PublishResult` per target and concurrency level:
`elapsed` is the wall time of the measured phase; `msgs_per_sec` and
`mib_per_sec` are throughput over that phase; `latency` is a
`Percentiles` block (`min`, `mean`, `p50`, `p90`, `p99`, `p999`, `max`) over
per-publish latency, computed with the nearest-rank method so every
reported percentile is a latency that actually occurred.

Recovery mode reports `acked` (publishes the broker confirmed before the
crash), `recovered` (of those, how many were readable after restart),
`restart` (time from relaunch to first record served), and `lost`
(`acked - recovered`; anything above zero is a durability violation).

## Running in Docker Desktop on Windows

The bind mount between Windows and the container makes fsync an order of
magnitude slower, which swamps the comparison. Run everything on a
container-local path instead of `/src`.

From the repository root, in Git Bash:

```
MSYS_NO_PATHCONV=1 docker run --rm -it \
  -v "$(pwd):/src" -w /src \
  -v pulse-gomod:/go/pkg/mod -v pulse-gocache:/root/.cache/go-build \
  -e GOPROXY=https://proxy.golang.org,direct -e GOFLAGS=-mod=mod \
  golang:1.26 bash
```

Inside the container:

```
go install github.com/nats-io/nats-server/v2@latest
go build -o /tmp/pulse-server ./cmd/pulse-server
mkdir -p /tmp/benchdir
cd bench
go run . -target=pulse,jetstream -mode=publish -n=2000 -conc=1,8 \
  -warmup=200 -sync=true -pulse-bin=/tmp/pulse-server -dir=/tmp/benchdir
```

`-pulse-bin=/tmp/pulse-server` and `-dir=/tmp/benchdir` keep both the
binary and the broker's data on the container's own filesystem, not the
Windows bind mount.

### Sample run

2026-09-02, in Docker Desktop on Windows (golang:1.26 container, both
binaries and data on the container filesystem, not the bind mount):

```
pulse      conc=1   n=2000    size=256          377 msg/s    0.09 MiB/s  p50=2.548ms   p99=4.495ms   p999=6.133ms   max=6.821ms
pulse      conc=8   n=2000    size=256          433 msg/s    0.11 MiB/s  p50=18.248ms  p99=22.699ms  p999=25.785ms  max=25.928ms
jetstream  conc=1   n=2000    size=256          354 msg/s    0.09 MiB/s  p50=2.731ms   p99=4.238ms   p999=5.226ms   max=5.4ms
jetstream  conc=8   n=2000    size=256          400 msg/s    0.10 MiB/s  p50=19.786ms  p99=36.135ms  p999=46.145ms  max=46.491ms
```

This ran with `-n=2000 -conc=1,8 -warmup=200 -sync=true`, small numbers
meant to prove the recipe works, not to characterize steady-state
performance. Both containers share one CPU-throttled Docker Desktop VM, so
absolute throughput here is not representative of bare-metal numbers; treat
it as a smoke test for the harness, not a performance claim.
