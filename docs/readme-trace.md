# README trace

One row per claim in the new README. `path:line` proves a claim about what
exists; a claim about behaviour cites the test that asserts it or a command
run in this session, quoted below the table it belongs to.

Environment for every command below: this machine, Docker Desktop 29.7.2,
`golang:1.26` container, `MSYS_NO_PATHCONV=1` in Git Bash, per the
`pulse-dev` skill. Repository HEAD at time of writing: `f01c247` on `master`.

## Old README audit

Every claim in the previous `README.md` (7690 bytes, last touched
2026-09-02), checked against current source.

| Claim | Verdict | Current evidence |
|---|---|---|
| At-least-once, total order per partition, no dedup, idempotent consumer required | kept | `docs/Guarantees.md:15-23` |
| Cursor is next offset, not last consumed | kept | `docs/Guarantees.md:86` |
| every-write acks after fsync; interval acks before, can lose up to sync-interval | kept | `docs/Guarantees.md:51`, `internal/infrastructure/config/config.go` sync-mode keys |
| No high watermark; a record can be delivered before durable | kept | `docs/Guarantees.md:151-170` |
| Not provided: exactly-once, clustering, replication, consumer groups, per-topic authz | kept | `docs/Guarantees.md:259-267` |
| TLS and mTLS via cert-file/key-file/client-ca-file | kept | `docs/Configuration.md:37-41`, `internal/infrastructure/grpc/server.go` |
| Append-only segment log, sparse indexes, checksummed batches, CRC recovery | kept | `internal/infrastructure/storage/engine/checksum/checksum.go:1-12` |
| Clean architecture: domain/application/adapters/infrastructure, ports | kept | `internal/domain`, `internal/application/ports`, `internal/adapters`, `internal/infrastructure` directory layout |
| "Single dependency for local dev: Go only, no Docker required" | **removed** | false on this machine: no `go` binary exists outside `golang:1.26`; every gate, build, and run here goes through Docker. Kept as a claim about the *code's* toolchain requirement would be fine, but the old wording claimed no Docker is needed at all, which does not hold for how this project is actually built here. |
| Deterministic shutdown: drain followers, GracefulStop, flush, monitor stays up | kept | `internal/server/server.go:171-174` |
| Monitor listener: /healthz, /readyz, /varz, /metrics on 9091 | kept | `docs/Configuration.md:26,176-178`; verified live below |
| Token auth via auth.tokens/auth.token-file/PULSE_AUTH_TOKENS, startup warning while off | kept | `docs/Configuration.md:40-41`, `internal/server/server.go:103` |
| Public Go client pkg/client, full-jitter retry on Unavailable, Subscribe resume | kept | `pkg/client/backoff_test.go:32-81`, `pkg/client/errors.go:19-32` |
| 1 to 256 partitions per topic, client.PartitionForKey | kept | `internal/domain/topic/topic.go:27` |
| Log compaction: compact topic, tombstones, offsets never renumbered, copy-and-swap | kept | `internal/infrastructure/config/config.go:124-132`; `tests/integration/compaction_test.go` |
| Quickstart: `make build` then `bin/pulse-server --config examples/config.yaml` | **changed** | builds fine inside `golang:1.26`, but the old README implied a bare local Go toolchain; rewritten below to show the container command actually run, and a Docker-only path since a release image and binaries are not published |
| Docker Compose brings up broker + Prometheus | kept | `examples/docker-compose.yaml` pulls `ghcr.io/yasser-ameur/pulse:latest`; run this session (R16): pulse `Up (healthy)`, prometheus up |
| Repository map table | kept | directory listing matches (`api/proto`, `pkg/api/pulse/v1/pulsepb`, `cmd/`, `internal/*`, `tests/integration/`, `docs/`) |
| Documentation links (10 files) | kept | all ten files exist in `docs/` |
| `make fmt/lint/test/test-race/coverage/coverage-check/image` | kept | `Makefile` targets present; `fmt`, `test`, `test-race` run in this session (see Gate below) |
| CI runs fmt, vet, lint, tests, race, coverage floor, bench, builds both binaries | kept | `.github/workflows/ci.yml` |
| Apache License 2.0 | kept | `LICENSE:1-3` |

Nothing was marked unverifiable; every carried claim resolved to current
source or a run below.

## Gate, this session

```
$ MSYS_NO_PATHCONV=1 docker run --rm -v ".../Pulse:/src" -w /src \
  -v pulse-gomod:/go/pkg/mod -v pulse-gocache:/root/.cache/go-build \
  -e GOPROXY=https://proxy.golang.org,direct -e GOFLAGS=-mod=mod \
  -e GOSUMDB=sum.golang.org -e CGO_ENABLED=1 golang:1.26 \
  sh -c "test -z \"\$(gofmt -l ./internal ./cmd ./pkg ./tests ./examples ./bench)\" \
  && go vet ./... && go test -race -count=1 ./..."
```

Output: every package `ok`, no `gofmt` diff, `go vet` silent. Full listing
in session log; representative lines:

```
ok  	github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/log	7.589s
ok  	github.com/pulse-stream/pulse/pkg/client	7.382s
ok  	github.com/pulse-stream/pulse/tests/integration	8.663s
```

## Published artifacts, checked this session

- `ghcr.io/yasser-ameur/pulse:latest` is published by `ci.yml` on every
  push to `master` (`type=raw,value=latest` plus `type=sha`, lines 76 to 78).
  An earlier draft of this trace called the image broken (`mkdir /data/meta:
  permission denied`): that verdict came from a local copy pulled on
  2026-08-19, not from the registry. R16 below is the fresh pull. Lesson kept
  in the readme skill: `docker pull` before judging a published image.
- `gh release list -R Yasser-Ameur/pulse` → empty, no releases.
- `git ls-remote --tags origin` → empty; the local tag `v0.1.0-phase1` was
  never pushed. No binaries are published anywhere.
- A fresh `docker build -t pulse:tryit .` from a clean clone (see Try it
  below) builds and runs correctly: `/healthz` and `/readyz` both return
  200. This is the try-it path in the README.

## Try it, this session, from an empty directory

```
$ git clone https://github.com/Yasser-Ameur/pulse.git
Cloning into 'pulse'...
$ cd pulse && git log -1 --oneline
f01c247 feat(broker): log when a compaction run starts
$ MSYS_NO_PATHCONV=1 docker build -t pulse:tryit .
...
#18 naming to docker.io/library/pulse:tryit done
$ MSYS_NO_PATHCONV=1 docker run -d --name pulse-try -p 9090:9090 -p 9091:9091 pulse:tryit
$ curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:9091/healthz
200
$ curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:9091/readyz
200
```

CLI, built and run through the same `golang:1.26` container this repo's own
gate uses, against the running broker via `host.docker.internal`:

```
$ MSYS_NO_PATHCONV=1 docker run --rm -v "<clone>:/src" -w /src \
  -v pulse-gomod:/go/pkg/mod -v pulse-gocache:/root/.cache/go-build \
  -e GOPROXY=https://proxy.golang.org,direct -e GOFLAGS=-mod=mod \
  --add-host=host.docker.internal:host-gateway golang:1.26 bash -c '
    go build -o /tmp/pulse-cli ./cmd/pulse-cli
    /tmp/pulse-cli --addr host.docker.internal:9090 topics create shipment-events
    /tmp/pulse-cli --addr host.docker.internal:9090 publish shipment-events --key order-4471 --message "{\"status\":\"packed\"}"
    /tmp/pulse-cli --addr host.docker.internal:9090 subscribe shipment-events --consumer fulfillment-worker
  '
created topic shipment-events (1 partitions)
published offset 0
0	2026-09-02T12:12:12Z	{"status":"packed"}
```

## Import path caveat

`go.mod` declares `module github.com/pulse-stream/pulse` (`go.mod:1`), but
the repository is hosted at `github.com/Yasser-Ameur/pulse`. Checked this
session: `curl https://github.com/pulse-stream/pulse` → 404;
`curl https://proxy.golang.org/github.com/pulse-stream/pulse/@latest` → 404.
The public client (`pkg/client`) does **not** resolve via a bare `go get`
from outside the repo; a caller needs a local clone plus a `replace`
directive, or `GOFLAGS=-mod=mod` against a vendored copy. The README's
client example is presented as API shape from inside a clone, and the
caveat is stated, not silently dropped.

## Demo GIF: `assets/demo.gif`

Producer: `assets/demo.tape`, run with
`docker run --rm -v "<dir>:/work" -w /work ghcr.io/charmbracelet/vhs:latest demo.tape`
against `linux/amd64` `pulse-server`/`pulse-cli` binaries built from this
session's gate build (`go build ./cmd/pulse-server`, `./cmd/pulse-cli`,
`CGO_ENABLED=0`). One task: create the `shipment-events` topic, start a live
`--follow` subscribe, publish three records with real order keys
(`order-4471`, `order-8823`) and JSON status payloads, watch them arrive.

Measured with Pillow this session: 1200×600 px, 773 frames, 30.92 s, 264715
bytes (0.25 MiB). Within the 12-40 s / under 2.5 MB / 960-1300 px bar.
Timestamps in frame: `12:05:41Z`, `12:05:47Z`, `12:05:53Z` (staggered, not
same-second). No placeholder or lorem text; `order-4471`/`order-8823` and the
JSON payloads are the same shapes used elsewhere in this repo's own
examples (`README.md` before this rebuild used `orders`/`user-42`).

## Benchmark: `assets/bench-dark.png`, `assets/bench-light.png`

Producer: `assets/bench-chart.py`, reading `assets/bench-publish.json`.
Harness run this session, interleaved (`bench/harness.go` runs pulse then
jetstream at each concurrency level in sequence), container-local paths per
`bench/README.md`'s Docker Desktop recipe, not the Windows bind mount:

```
$ MSYS_NO_PATHCONV=1 docker run --rm -v ".../Pulse:/src" -w /src \
  -v pulse-gomod:/go/pkg/mod -v pulse-gocache:/root/.cache/go-build \
  -e GOPROXY=https://proxy.golang.org,direct -e GOFLAGS=-mod=mod golang:1.26 bash -c '
    go install github.com/nats-io/nats-server/v2@latest
    go build -o /tmp/pulse-server ./cmd/pulse-server
    cd bench && go build -o /tmp/bench .
    /tmp/bench -target=pulse,jetstream -mode=publish -n=3000 -conc=1,8,32 \
      -warmup=300 -sync=true -pulse-bin=/tmp/pulse-server -dir=/tmp/benchdir \
      -out=/scratch/bench-publish.json
  '
pulse      conc=1   n=3000   size=256   243 msg/s   p50=3.14ms   p99=15.23ms
pulse      conc=8   n=3000   size=256   235 msg/s   p50=29.30ms  p99=119.40ms
pulse      conc=32  n=2976   size=256   282 msg/s   p50=113.63ms p99=164.87ms
jetstream  conc=1   n=3000   size=256   266 msg/s   p50=2.84ms   p99=15.40ms
jetstream  conc=8   n=3000   size=256   291 msg/s   p50=25.02ms  p99=62.90ms
jetstream  conc=32  n=2976   size=256   356 msg/s   p50=84.97ms  p99=128.49ms
```

Full JSON in `assets/bench-publish.json`. Conditions the README states
plainly: same machine, Docker Desktop on Windows, both binaries and data on
the container filesystem, `n=3000` publishes per point, 256-byte payload,
`sync-mode: every-write` (fsync every batch) on both sides, single run, no
repeats or warm/cold averaging beyond the harness's own 300-publish warmup.
This says the two are in the same class on this box under this one
configuration; it is not a general throughput claim for either system, and a
second run on this same machine produced different absolute numbers (349,
435, 445 msg/s for pulse; not committed) while keeping pulse and jetstream in
the same band, which the README states directly instead of implying
precision the single run doesn't have.

## Badges, checked this session

| Badge | URL | Status |
|---|---|---|
| CI | `https://github.com/Yasser-Ameur/pulse/actions/workflows/ci.yml/badge.svg?branch=master` | 200 |
| License | `https://img.shields.io/github/license/Yasser-Ameur/pulse` | 200, live from repo metadata |
| Go version | `https://img.shields.io/badge/go-1.26-00ADD8` | 200, static, matches `go.mod:3` (`go 1.26.5`) at time of writing |

No release/version badge: nothing is tagged on the remote to point it at.

## quickstart.exe, bin/, data/

`git ls-files` for `quickstart.exe`, `bin/`, `data/` returns nothing; all
three are excluded by `.gitignore` (`*.exe`, `/bin/`, `/data/`). None
tracked, so none removed or reported as a leak.

## R16. The published image, pulled fresh (2026-09-02)

```
$ docker pull -q ghcr.io/yasser-ameur/pulse:latest
$ docker image inspect ghcr.io/yasser-ameur/pulse:latest --format 'revision={{index .Config.Labels "org.opencontainers.image.revision"}} created={{.Created}}'
revision=f01c24731a52ee65bd417365415178b814743cae created=2026-09-02T08:22:22.820151686Z
$ docker run -d --name pulse-tryit -v pulse-tryit-data:/data -p 19090:9090 -p 19091:9091 ghcr.io/yasser-ameur/pulse:latest
$ curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:19091/healthz ; curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:19091/readyz
200 200
$ docker compose -f examples/docker-compose.yaml -p pulsecheck up -d && docker compose -f examples/docker-compose.yaml -p pulsecheck ps
pulsecheck-pulse-1        Up 9 seconds (healthy)   0.0.0.0:9090-9091->9090-9091/tcp
pulsecheck-prometheus-1   Up 8 seconds             0.0.0.0:9092->9090/tcp
```

`gh run list` shows the CI run on `f01c247` (push, 2026-09-02T08:17:55Z)
completed with success, so the published revision equals `HEAD`. The try-it
block in the README is therefore the pull, and the clone serves only the CLI
build.

## R17. Audit re-verification of the try-it block (2026-09-02, later session)

The full README "Try it" sequence was re-run end to end in a fresh audit
session against the current `master` (`10ffe9d`): `docker pull
ghcr.io/yasser-ameur/pulse:latest`, `docker run` the broker, `curl` `/healthz`
(200), then the CLI build-and-run block from a fresh `git clone` through the
`golang:1.26` container. The command sequence and its structure matched
README.md:19-39 exactly; only the record's timestamp differed, since it is
wall-clock time at publish, not a stored value:

```
created topic shipment-events (1 partitions)
published offset 0
0	2026-09-02T15:03:25Z	{"status":"packed"}
```

README.md:44-47 updated to this run's timestamp.

## R18. Module path fixed (2026-09-02, later session)

The "Import path caveat" above (R at line 114) is now historical. `go.mod:1`
was renamed from `module github.com/pulse-stream/pulse` to `module
github.com/Yasser-Ameur/pulse`, and every import across the repo's `.go`
files, `Makefile`, `Dockerfile`, `.github/workflows/release.yml`,
`docs/Client.md` and `docs/Protocol.md` was updated to match. The module path
now matches the repository host, so `go get github.com/Yasser-Ameur/pulse/pkg/client`
will resolve once this commit is pushed; it cannot be checked against the live
proxy from an unpushed local commit, so that step is not claimed as run this
session. The full gate (gofmt, go vet, go test -race) was re-run against the
rename in the `golang:1.26` container this session, all packages `ok`; the
bench module (`bench/go.mod`, its own `replace` target) was rebuilt and
vetted the same way; `docker build -t pulse:test .` and a second build with
`--build-arg VERSION=v0.2.0-test` both succeeded, and `docker run --rm
pulse:vtest --version` printed `v0.2.0-test`, confirming the ldflags path
still finds `internal/server.Version` after the rename; `golangci-lint run
./...` (v2.12.2) reported 0 issues.
