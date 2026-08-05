# Concurrency

This document is the concurrency contract for Pulse: who owns which goroutines
and channels, where locks are taken, how backpressure propagates, and the exact
shutdown sequence. The guiding principles are determinism, minimal locks, and
obvious ownership. We deliberately avoid speculative parallelism.

## 1. Goroutine ownership

| Goroutine | Owner | Lifecycle |
|---|---|---|
| gRPC server accept loop | `infrastructure/grpc.Server` | Started by `server.Run`, stopped by `GracefulStop`. |
| Per-RPC handler | gRPC runtime | One per in-flight unary RPC; owned by grpc-go. |
| Per-subscription reader | `services.Subscriber` | One per active `Subscribe` stream; exits when the stream ends or the broker stops. |
| Log data-readiness notification | engine `Log` | None — implemented with channel close/replace (below), no goroutine. |

Pulse does **not** spawn a background writer goroutine per log. Writes happen
synchronously on the caller (the publish handler) under the log's writer lock,
which is the simplest way to keep the durable-order invariant:

> records are durable in the same order their offsets are assigned.

## 2. Locking

- **`LogRegistry`** (`application/services`) owns a `sync.RWMutex` over the
  topic → log map. `Get` takes a read lock; `Register`/`Unregister`/`CloseAll`
  take the write lock.
- **Each `Log`** (engine) owns its own `sync.RWMutex`:
  - write lock for `Append` (append + fsync + index append + data-ready notify),
    `Truncate`, and `Close`;
  - read lock for `Read` and `NextOffset`.
- **Metadata store** implementations are internally synchronized (Pebble is
  thread-safe; the in-memory store uses a mutex).
- **Broker state machine** (`services.Broker`) is guarded by its own mutex;
  state transitions are validated against the allowed transition table.

Because publish and subscribe touch different partitions' logs and the registry
read lock is held only for the map lookup, unrelated partitions never contend.
A slow subscriber blocks only its own reader goroutine (on the gRPC transport
window) and never holds a log lock while blocked.

## 3. Channels

- **Log data-ready**: the engine keeps a `chan struct{}` field. On each
  successful append it closes the current channel and replaces it with a fresh
  one (under the write lock). Readers call `Notify()` to obtain the current
  channel, then `select` on it plus their context. On close they call
  `Notify()` again and re-read. This gives push-style wakeups with no
  per-reader goroutines or producer-side fan-out.
- **Subscription reader → gRPC stream**: no intermediate channel. Records are
  decoded under a short read lock, then `Send` on the gRPC stream. gRPC's
  transport window is the bounded queue and provides backpressure.
- **Shutdown signal**: `context.Context` cancellation is the universal stop
  signal; every blocking wait selects on its context.

## 4. Backpressure

Backpressure flows in the direction the data flows:

```
publish handler ──append──▶ log writer lock ──fsync──▶ ack
                                       ▲
slow/full disk, slow transport          │ append blocks → handler blocks → client sees latency
```

- **Publish**: bounded by the log write lock and fsync; a full disk or a slow
  filesystem throttles producers directly. There is no unbounded queue.
- **Subscribe**: a slow consumer fills its HTTP/2 window; its reader blocks on
  `Send` and stops consuming the log. Other consumers and producers are
  unaffected because the reader holds no locks while blocked.
- **Ack**: unary, synchronous, cheap (one metadata write).

## 5. Memory

- Record payloads are copied out of decoded batches; the log never hands out
  references into its buffers, so no record outlives its batch and the GC can
  reclaim freely.
- Read results are bounded by `limit` and `maxBytes` supplied by the caller
  (the subscriber uses the configured read size).
- The index for a segment is loaded once and kept resident (a few MiB per
  segment); sealed segment data files are not memory-mapped in Phase 1.

## 6. Shutdown sequence

Exactly one documented sequence, driven by `services.Broker.Shutdown(ctx)`:

1. Transition `Running → Draining`. The gRPC server stops accepting new RPCs
   and drains in-flight ones (`GracefulStop` with the configured timeout).
2. Health status flips to `NOT_SERVING` so load balancers and probes stop
   routing work.
3. Transition `Draining → Stopping`.
4. All subscription readers are canceled via their contexts.
5. Every open log is `Sync`ed (final durability flush) then `Close`d.
6. The metadata store is closed (Pebble flushes its WAL).
7. Transition `Stopping → Stopped`; the process may exit.

If the graceful deadline expires, `Stop()` force-closes remaining RPCs and
proceeds to steps 4–6; data safety is unaffected because fsync-before-close
always runs.

## 7. Determinism

- Time is injected via `ports.Clock`; production uses `SystemClock`, tests use
  a fixed clock, so nothing in the application depends on wall-clock ordering.
- Log ordering is total under the writer lock; concurrent publishes interleave
  at batch granularity, never within a batch.
- Recovery is deterministic: the same data directory always produces the same
  log state after any crash.

## 8. What we deliberately avoid

- No per-record locks, lock striping, or lock-free structures in Phase 1. The
  single-writer mutex is provably correct and measurable; if benchmarks show it
  is the bottleneck, the documented upgrade path is sharded append queues, not
  a redesign.
- No background batching/flush goroutines for the log in Phase 1 (the fsync
  policy is synchronous or timer-driven at the storage layer only).
- No work queues with unbounded capacity anywhere.
