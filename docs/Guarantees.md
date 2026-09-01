# Guarantees

What Pulse promises, what it refuses to promise, and the one place where the
code is weaker than a reader would assume. Every statement here is a statement
about the code as it exists today, not about the roadmap. Where the code does
not do something, this document says so rather than describing an intention.

`docs/Storage.md` §1 states the four *storage* guarantees (durability,
immutable offsets, per-partition order, torn-write recovery). This document
states the *delivery* guarantees that sit on top of them, and is the one to
read before writing a consumer.

## The statement

> Pulse delivers each record **at-least-once**, ordered **totally within a
> partition**, deduplicated **nowhere — the consumer must be idempotent**,
> visible to readers **as soon as the write reaches the page cache, which is
> before it reaches stable storage**, and on an ambiguous publish it reports an
> error that does **not** distinguish "not appended" from "appended".

The rest of this document is that sentence, slot by slot.

## 1. Delivery — at-least-once

The broker never re-pushes a record on its own: there is no redelivery timer,
no nack, and no in-flight tracking. Duplicates arise from **resume**, and they
are unavoidable:

1. The consumer receives records `N..M` and processes them.
2. It crashes before calling `Ack(M+1)`.
3. On restart it resumes from its stored cursor, which is at or below `N`.
4. Records it already processed are delivered again.

The gap between "processed" and "acknowledged" is the consumer's own code, and
nothing in the broker can close it. **Consumers must be idempotent.** See §5.

The complement also holds: a consumer that acks *before* processing converts
this into at-most-once and will silently lose work on a crash. Ack after the
side effect is durable, never before.

A subscription with an empty `consumer` field is not tracked at all: it replays
from offset 0 on every run.

## 2. Durability — two guarantees, chosen by `sync-mode`

`storage.sync-mode` selects between two genuinely different promises. Both are
supported; they are not tuning knobs on one guarantee.

| `sync-mode` | Publish is acknowledged | Loss window on machine crash |
|---|---|---|
| `every-write` (default) | after `fsync` of the batch | none for acknowledged publishes |
| `interval` | after the write reaches the page cache | up to `storage.sync-interval` (default 100ms) of acknowledged publishes |

`every-write` is the default and is what "durable by default" in the README
means. Under `interval`, `Log.Append`
(`internal/infrastructure/storage/engine/log/log.go`) performs no `Sync` at
all; a background flusher syncs on a ticker. A publish acknowledged under
`interval` is *not* yet durable, and an OS or machine crash within the interval
loses it. That is the trade being bought, and it must be stated wherever a
deployment chooses it.

Consumer cursors are durable in both modes: `PebbleMetadataStore.SaveCursor`
writes with `pebble.Sync`, so `Ack` does not return until the cursor is on
stable storage.

Crash recovery truncates a torn tail at the last CRC-valid batch
(`docs/Storage.md` §7), so a lost record is lost cleanly — never half-read.

## 3. Cursors and the resume point

**A stored cursor is the NEXT offset to consume, not the last one consumed.**

`Subscriber.startPosition` hands the cursor straight to `Log.Read`, which
returns records *at or after* that offset. A cursor of `N` therefore delivers
record `N` first.

- Processed through offset `N` → call `Ack(N+1)`.
- Calling `Ack(N)` redelivers record `N` on every resume.

Acks are monotonic: an offset at or below the stored cursor is ignored and the
existing cursor is returned, so a late or duplicated `Ack` cannot rewind a
consumer. An offset above the log's end is accepted by `Ack` but makes the next
`Subscribe` fail with `OutOfRange`.

`AckRequest.offset` in `api/proto/pulse/v1/pubsub.proto` documents this
correctly today: the next offset the consumer wants, not the last one it
processed.

## 4. Ordering

Order is **total within a partition** and undefined across partitions. Appends
serialize on a single writer lock and offsets are assigned under it, so records
in a partition are contiguous and never reordered, and a batch is written
atomically.

Two things this does not mean:

- **Message keys do not route.** `Publish` takes an explicit partition id;
  there is no partitioner and no key hashing anywhere in the code. `Key` is
  advisory metadata carried through unchanged. Two messages with the same key
  land in the same partition only because the caller put them there.
- **Producer order is not preserved across concurrent publishes.** Order is the
  order in which batches acquire the writer lock, not the order in which
  clients called `Publish`.

Today every topic has exactly one partition —`TopicManager.CreateTopic` rejects
any other count — so per-partition order is currently total per topic. That
stops being true the moment multi-partition topics land, and code should not
depend on it.

## 5. Duplicates and idempotency

**There is no deduplication anywhere in Pulse.** Not on publish, not on
delivery.

- `event_id` is a ULID assigned by the broker when the client leaves it empty.
  It is stored and returned, and it is never compared against anything. It is
  not a dedup key.
- `producer.ProducerID` exists in the domain model as reserved capacity for a
  future idempotent producer. Nothing reads it today.
- Retrying a `Publish` that timed out appends the batch a second time, at new
  offsets.

Idempotency is therefore entirely the consumer's job, and it needs a key the
*caller* supplies or that is derived from the payload — a business key such as
an order id, not `event_id` and not the offset, since a republish gets a fresh
one of each. Deduplicate at a boundary you can name: a unique index, or a
compare-and-set on a version.

## 6. The UNKNOWN outcome

A failed `Publish` has two shapes and the client cannot tell them apart:

- The RPC failed or timed out in transit. The batch may or may not have been
  appended.
- The append succeeded but `fsync` failed. `Log.Append` returns
  `(baseOffset, err)`; the caller's error path discards the offset, but the
  records are already in the log's address space and are already visible to
  subscribers. The publisher sees a failure for records that consumers will
  receive.

Treat a publish error as UNKNOWN, not as failure. Retrying is correct, and
duplicates it — which is fine, because §5 already requires consumers to tolerate
them.

## 7. Known non-guarantee: reads can outrun the fsync

This is a real weakness in the code, recorded here rather than glossed.

In `internal/infrastructure/storage/engine/log/log.go`, `Append` does:

```go
l.nextOffset += offset.Offset(len(batch.Records))
close(l.notify)                  // wake subscribers
l.notify = make(chan struct{})
if l.cfg.SyncMode != SyncInterval {
        if err := l.active.SyncData(); err != nil { ... }   // fsync AFTER the wake
}
```

Subscribers are woken **before** the batch is fsynced, and `l.nextOffset` — the
bound `Log.Read` uses to decide what is readable — advances before it too.
There is **no high-watermark** in the log: `Read` is bounded by the log end
(LEO), so nothing can hold a record back until it is durable. The snapshot code
records this directly (`snapshot.go`: "LEO is also the high watermark").

The consequence, per mode:

- **`interval`**: real and routine. `Append` never syncs, so every read of a
  recent record is a read of data that is not on stable storage. A consumer can
  process a record that a machine crash then erases, and after recovery that
  record does not exist and its offset is reassigned to a different record.
- **`every-write`**: the window is closed today, but only incidentally. The
  wake happens inside `l.mu.Lock()` and `Log.Read` takes `l.mu.RLock()`, so a
  woken subscriber blocks until `Append` releases the lock, which is after the
  `Sync`. Nothing in the code states this as an invariant, and the obvious
  throughput change — moving the `fsync` out of the writer lock — opens the
  window without touching a single line that looks related.
- **`every-write`, failed fsync**: open today. On the `Sync` error path the
  offset has already advanced and the bytes are already in the page cache, so
  the record is delivered even though the publish reported an error (§6).

**This is not fixed.** Fixing it means introducing a real high watermark and
publishing it only after `fsync`, which costs latency on every read of the tail.
That is a design decision with a cost, and it is the user's to make. Until then
the honest statement is: *Pulse does not guarantee that a delivered record is
durable at the moment it is delivered.*

`internal/domain/partition` contains only an id and a lifecycle state; it owns
no LEO/HW (log-end-offset/high-watermark) rules. Nothing in the current
codebase claims otherwise. That distinction is an intended design for a
future replicated partition, not a gap in today's single-copy log.

## 8. What this does not give you

Deliberate omissions. Each is a decision, not a gap, and `docs/Roadmap.md` maps
the ones that are scheduled to the phase that adds them.

- **Exactly-once delivery.** Explicitly out of scope (Roadmap "Out of scope
  until explicitly scoped"). At-least-once plus consumer-side idempotency is
  the contract.
- **Durability at the moment of delivery.** §7.
- **Clustering and replication.** Single node, single copy. Deferred to Phase 5
  (etcd/raft, one group per partition). There is no replication factor in
  force, no failover, and losing the data directory loses the data.
- **Consumer groups.** Deferred to Phase 4. `Ack` tracks one cursor per
  `(consumer, topic, partition)` and nothing enforces exclusivity: two
  processes sharing a consumer id both read every record and race on the
  cursor. There is no lease, no fencing token, and no assignment protocol.
  Coordination is the caller's problem today.
- **`Nack`, retry policies, and dead-letter queues.** Deferred to Phase 4. A
  consumer that cannot process a record has one option: do not advance its
  cursor.
- **Replay by timestamp.** Not exposed. Resume is by offset only.
- **TLS.** Not enabled; the transport is plaintext gRPC. Deferred to Phase 3.
  `client.Dial` takes variadic `grpc.DialOption`s, so a caller can already
  supply credentials, but `infrastructure/grpc.NewServer` exposes no way to
  pass `grpc.Creds` — the server half still has to be built.
- **Authentication and authorization.** None. Any client that can reach the
  listener can create, delete, publish to, and read every topic, and can move
  any consumer's cursor. Do not expose the listener outside a trusted network.
- **Multi-partition topics.** `CreateTopic` accepts a partition count of 1 and
  rejects anything else.
- **Public API stability.** Out of scope until explicitly scoped.
