# Client

`pkg/client` is the public Go client for the Pulse broker. It speaks the
`pulse.v1` gRPC protocol ([Protocol.md](Protocol.md)) and exposes plain public
types, so any external Go program can depend on it without importing anything
under `internal/`. `pulse-cli` (`internal/adapters/cli`) is built on it.

## Install

```bash
go get github.com/pulse-stream/pulse/pkg/client
```

## Dial

```go
c, err := client.Dial("127.0.0.1:9090")
if err != nil {
    log.Fatal(err)
}
defer c.Close()
```

`Dial` options (`pkg/client/client.go`):

| Option | Effect |
|---|---|
| `WithTLS(cfg *tls.Config)` | Dials with `cfg` instead of an insecure connection. |
| `WithDialOptions(opts ...grpc.DialOption)` | Appends raw `grpc.DialOption`s after the client's own transport and message-size options. |
| `WithCallTimeout(d time.Duration)` | Overrides `DefaultCallTimeout` (30s), applied to a unary call only when its context carries no deadline of its own. |
| `WithMaxMsgBytes(n int)` | Overrides `DefaultMaxMsgBytes` (64 MiB) for both send and receive. |
| `WithToken(token string)` | Attaches `token` as per-RPC credentials, sent as gRPC metadata key `authorization: Bearer <token>` on every call. |

Dial with `WithToken` when the broker has `auth.tokens` or
`auth.token-file` set. A missing or wrong token surfaces from any RPC as
`client.ErrUnauthenticated` (`pkg/client/errors.go`), matched with
`errors.Is`, the same way `ErrNotFound` and the other sentinels are.

## Publish

```go
offsets, err := c.Publish(ctx, "orders", 0, client.Message{
    Key:     "user-42",
    Payload: []byte(`{"sku":"a1"}`),
})
```

`Publish` accepts a variadic list of `client.Message` and returns one offset
per message, aligned by index. If the broker returns `codes.Unavailable`
(returned while it is draining), `Publish` retries automatically with
exponential backoff: 50ms initial, doubling, capped at 2s, until the caller's
context is done; a context without a deadline is bounded by the call timeout
(`pkg/client/publish.go`). Each wait uses full jitter: a uniform random
duration in `[0, d]` rather than `d` itself (`jitter`, `pkg/client/publish.go`),
so a batch of clients retrying together does not resynchronize into a
thundering herd. Any other error, or running out of budget, returns
immediately.

### Routing by key

A multi-partition topic needs the caller to pick a partition; the broker does
not hash keys for you. `client.PartitionForKey(key string, partitions int)
int32` (`pkg/client/partition.go`) hashes `key` with FNV-1a and maps it into
`[0, partitions)`, so the same key always lands on the same partition for a
given partition count:

```go
p := client.PartitionForKey("user-42", topic.Partitions)
offsets, err := c.Publish(ctx, "orders", p, msg)
```

`pulse-cli publish --key` does exactly this: when `--key` is set and
`--partition` is not, the CLI resolves the topic's partition count and calls
`PartitionForKey` itself (`internal/adapters/cli/pubsub.go`).

## Subscribe

```go
var next int64
err := c.Subscribe(ctx, "orders", 0, client.SubscribeOptions{
    Consumer: "warehouse",
    Follow:   true,
}, func(r client.Record) error {
    fmt.Println(r.Offset, string(r.Message.Payload))
    next = r.Offset + 1
    return nil
})
```

`SubscribeOptions`:

- `Consumer`: when set and `StartOffset` is nil, the stream resumes from
  this consumer's stored cursor.
- `StartOffset`: overrides any stored cursor; nil means "use the cursor (or
  0)".
- `Follow`: `false` replays to the current end of the log and returns;
  `true` streams new records as they arrive and never returns until `ctx` is
  canceled or `fn` returns an error.

**Resume semantics** (`pkg/client/subscribe.go`): with `Follow: true`,
`Subscribe` transparently redials the stream when it fails with
`codes.Unavailable`, or with a transport error that never reached the gRPC
status layer (e.g. a dropped connection). Context cancellation is never
treated as transient. Each redial resumes from the offset one past the last
record actually delivered to `fn`, or from the original start if nothing was
delivered yet, using the same full-jitter backoff schedule as `Publish`
(`pkg/client/subscribe.go`), reset to the 50ms floor after every successful
receive, so a later disconnect starts its own backoff from scratch. Any other
error, or `fn` itself returning an error, ends `Subscribe` immediately without
retrying.

## Ack: N+1

```go
cursor, err := c.Ack(ctx, "warehouse", "orders", 0, next)
```

A stored cursor is the **next** offset to consume, not the last one consumed.
`Ack`'s `next` argument is one past the last record the consumer finished
processing: after processing offset `N`, call `Ack(..., N+1)`. Acking `N`
itself redelivers record `N` on the next resume. This is exactly what the
`Subscribe` loop above computes into `next`.

## Error sentinels

`pkg/client/errors.go` maps gRPC status codes to exported sentinels, matched
with `errors.Is` against any error a `Client` method returns; `status.Code`
still recovers the original gRPC status from the same error via `Unwrap`.

| Sentinel | gRPC code |
|---|---|
| `client.ErrNotFound` | `NotFound` |
| `client.ErrAlreadyExists` | `AlreadyExists` |
| `client.ErrInvalidArgument` | `InvalidArgument` |
| `client.ErrUnavailable` | `Unavailable` |
| `client.ErrUnauthenticated` | `Unauthenticated` |

```go
if _, err := c.Publish(ctx, "orders", 0, msg); errors.Is(err, client.ErrUnavailable) {
    // broker is draining; Publish already retried internally
}
```

## TLS

```go
c, err := client.Dial(addr, client.WithTLS(&tls.Config{
    RootCAs: pool, // trust this CA
}))
```

For mTLS, add the client certificate to the same `tls.Config`:

```go
cert, _ := tls.LoadX509KeyPair("client-cert.pem", "client-key.pem")
c, err := client.Dial(addr, client.WithTLS(&tls.Config{
    RootCAs:      pool,
    Certificates: []tls.Certificate{cert},
}))
```

See [Configuration.md](Configuration.md) for generating the certificates.

## CLI TLS flags

`pulse-cli` (`internal/adapters/cli/root.go`) exposes the same settings as
persistent flags on every subcommand:

| Flag | Effect |
|---|---|
| `--tls-ca` | Trust this CA certificate file; its presence alone enables TLS. |
| `--tls-cert` | Client certificate file, for mTLS. |
| `--tls-key` | Client key file, for mTLS. |
| `--tls-skip-verify` | Skip server certificate verification (dev only). |
| `--token` | Bearer token to authenticate with, defaulting to `PULSE_TOKEN` (`internal/adapters/cli/root.go`). |

```bash
pulse-cli --addr pulse.internal:9090 --tls-ca ca-cert.pem topics create orders
```

Unary commands (`topics`, `publish`, `ack`, `info`) are bounded by a 30s
timeout; `subscribe --forever` opts out so it can run until interrupted
(`unaryContext`, `internal/adapters/cli/root.go`).

## A complete example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/pulse-stream/pulse/pkg/client"
)

func main() {
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

    var next int64
    err = c.Subscribe(ctx, "orders", 0, client.SubscribeOptions{Consumer: "cli"},
        func(r client.Record) error {
            fmt.Println(r.Offset, string(r.Message.Payload))
            next = r.Offset + 1
            return nil
        })
    if err != nil {
        log.Fatal(err)
    }
    if _, err := c.Ack(ctx, "cli", "orders", 0, next); err != nil {
        log.Fatal(err)
    }
}
```

`pkg/client/example_test.go` carries a runnable, compiled (but not executed)
`Example()` against an in-process broker started with `internal/testutil`.
