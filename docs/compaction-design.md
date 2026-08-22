# Log compaction design (Phase 2/3)

This document specifies the crash-safe log compaction pass for compacted topics
(`topic.Config.Cleanup == CleanupCompact`). It is the design contract for the
implementation; the final prose lives in `docs/Storage.md` §8.

## 1. Goals and non-goals

Goals:

- Keep the **last record per key** and drop superseded values, streaming the
  data through bounded memory.
- Preserve the **address space**: offsets of surviving records never change and
  are never renumbered; LEO of every segment and of the log is unchanged.
- Make compaction **crash-safe at every step** (copy-and-swap, atomic renames,
  deterministic recovery) and **non-blocking** for publishers.
- Keep compaction a pure **storage concern**: no consumer, cursor, transport,
  or CLI awareness anywhere in the engine.

Non-goals (explicitly out of scope):

- Combined `delete` + `compact` policies.
- Consumer-group-aware compaction (protecting the segment a live cursor is in).
- Changing the topic cleanup policy after creation (the mode is immutable for
  the life of the topic; enforcement below relies on that).
- Memory mapping of sealed segments.

## 2. Semantics

- **Keyed records only.** A record with `Message.Key == ""` is never removed and
  never deduplicated; it is re-encoded verbatim.
- **Newest wins.** Within the compacted region (all sealed segments plus the
  durable prefix of the active segment), only the record with the highest offset
  for each key survives. Records whose key has a newer occurrence anywhere in the
  region are dropped.
- **Offsets immutable.** Survivors keep their original offsets. A compacted
  segment spans the same `[base, nextOffset)` range it always did, but the data
  inside is a sparse subset — holes are expected. Readers at a removed offset
  simply see the next surviving record (the existing `Read` path already skips
  `r.Offset < from`).
- **Tombstones.** A tombstone is a keyed record with a nil payload (encoded as
  `nullField`, see §4). While a tombstone is younger than the tombstone
  retention window it is retained and suppresses every older record for its key.
  Once the tombstone is older than the window it is dropped too (its older
  values are dropped in the same pass, so nothing is resurrected).
- **Batch boundaries.** Batches are re-encoded; the batch is the persistence
  unit but not a compaction unit. Original batch grouping is not preserved.
- **At-least-latest guarantee.** Because the active segment is never rewritten,
  a key whose newest record is in the active segment always retains that record;
  sealed segments are fully deduplicated against everything newer. A reader that
  reads the whole log always observes the newest value for every key.

## 3. Placement

Compaction is implemented in the storage engine. The engine exposes it as a new
method on the `storage.Log` port:

```go
type CompactionResult struct {
    Segments         int   // sealed segments rewritten
    BytesBefore      int64
    BytesAfter       int64
    TombstonesRemoved int
}

// Compact deduplicates sealed segments of a compacted log. It never touches
// the active segment and never changes offsets or LEO. It is safe for
// concurrent use and returns a zero result when there is nothing to do.
Compact(ctx context.Context) (CompactionResult, error)
```

The *broker* decides *when* to call it, exactly like retention: a maintenance
loop walks topics and calls `Compact` only for topics whose `Cleanup ==
CleanupCompact`. The engine has no knowledge of topics, consumers, or cursors —
the existing `CleanupCompact` value in `topic.Config` is the only wiring, and it
is already accepted by `Config.Validate`. The scheduler never compacts the
active segment (enforced in the engine) and runs incrementally (a bounded number
of segments per call).

Rationale: this mirrors the retention design (broker triggers `Log.Trim`), keeps
the port a single seam, and keeps all compaction mechanics where they can be
tested in isolation. A future engine-internal scheduler would be a drop-in.

## 4. Wire-format changes (additive, backward compatible)

Two changes to `codec` are required. Both are readable by the existing decoder
paths, so old files and new files interoperate and **no format version bump** is
needed.

1. **Null value (tombstone) encoding.** `encodeRecord` currently writes a value
   with `appendBytes` (length prefix, never `nullField`). Change it so a nil
   `Message.Payload` encodes as `nullField` (`0xFFFFFFFF`); a zero-length
   non-nil payload stays a zero-length value and is *not* a tombstone. The
   decoder already maps `nullField` to a nil slice.

2. **Sparse-offset batches.** `decodeRecord` rejects a record whose offset delta
   is `>= recordCount`, on the assumption that a batch's offsets are contiguous.
   Compacted batches re-encode survivors at their original offsets, so a batch
   can legitimately cover non-contiguous offsets (e.g. records at 5 and 7 with 6
   removed). Add a batch flag bit (bit 0x0008, outside the compression bits
   `0x0007`) meaning "sparse offsets". `EncodeBatch` sets it automatically when
   any record's delta differs from its index; `DecodeBatch` passes it to
   `decodeRecord`, which enforces the old strict bound only for non-sparse
   batches and a generous sanity bound (`delta < 1<<31`) for sparse ones. CRC
   remains the real integrity check.

## 5. The compaction algorithm (per call to `Log.Compact`)

One call compacts a **bounded** slice of a compacted log. The log keeps an
in-memory `compacting` flag (under the log mutex) so concurrent calls serialize;
a call that finds the flag already set returns a zero result.

1. **Candidate selection.** Under the read lock, take the sealed segments
   (all but the active), oldest first. If none, return a zero result.

2. **Pass 1 — dedupe map.** Stream every sealed segment oldest→newest, then the
   *durable prefix* of the active segment (a best-effort scan from position 0 up
   to the active size captured at the start; the prefix is immutable because
   appends only grow the file, and a torn batch at the tail simply stops the
   scan). For each keyed record record `(offset, key, tombstone, ts)` keep the
   highest offset seen. Keyless records are ignored by the map (always kept).
   This builds, for every key, its newest occurrence: offset, whether it is a
   tombstone, and its timestamp. Memory is `O(distinct keys)` and the data is
   streamed through small fixed buffers — never buffered by size. The map is
   capped by `MaxCompactionKeys` (default `1<<18`); a segment range that would
   overflow the cap aborts the run (a later run retries), so memory is bounded
   regardless of segment size.

3. **Pass 2 — rewrite candidates, oldest first.** For each sealed segment, stream
   its batches and decide each record:
   - keyless → keep;
   - offset is the map's newest for its key and it is a value → keep;
   - offset is the map's newest for its key and it is a tombstone → keep only if
     younger than `TombstoneRetention`, else drop;
   - offset is older than the map's newest for its key → drop.
   Survivors are re-encoded into fresh batches (bounded by `CompactBatchRecords`
   / `CompactBatchBytes`, defaults 1000 records / 1 MiB), written to a temp file
   in the same directory. At most `MaxSegmentsPerRun` (default 4) segments are
   rewritten per call — the call is incremental and short.

4. **Gain gate.** If the projected output would not shrink the segment by at
   least `MinCompactGain` (default 0.1, i.e. 10%) and no expired tombstones were
   removed, the temp file is discarded and the segment is left untouched. This
   prevents rewriting near-clean segments. (An untouched segment remains eligible
   in a later run, which is what eventually removes values superseded by records
   that arrived afterwards.)

5. **Empty segments.** If a sealed segment's every record is dropped, the segment
   itself is deleted (closed and removed from the list), like a trim. Its offset
   span leaves no survivors, so removing it is safe and offset-preserving.

## 6. Copy-and-swap commit protocol

For each rewritten segment `S` at base `B` with LEO `L` (its original
`NextOffset`, never changed):

1. Write the compacted data to a temp file in the partition dir
   (`.tmp-compact-*`), fsync it. The name never ends in `.log`, so
   `filesystem.SegmentFiles` ignores it during recovery.
2. Build the sparse index entries from the new layout (`relativeOffset =
   batchBase - B`, `relativePosition`), write the index to a temp file
   (`.tmp-compact-*.index`), fsync it.
3. Take `scanMu` and then the **log writer lock**, and re-verify `S` is still
   present and sealed. `scanMu` is what guarantees no reader is mid-scan on `S`
   — the writer lock no longer does, because a read holds it only to snapshot —
   so the handle can be closed (Windows cannot rename over an open file).
4. `S.Close()` — this re-syncs the *old* index, which is fine because it happens
   before the new files are renamed over it.
5. `os.Rename(tempData, B.log)`; `os.Rename(tempIndex, B.index)`;
   `filesystem.SyncDir(dir)`.
6. Open the renamed data file, `RecoverFrom(size, L, entries)`, and swap it into
   `l.segments` in place of `S`. Clear the `compacting` flag.

LEO, `l.nextOffset`, the snapshot, and `Notify` are all untouched: only sealed
segments changed, and their LEO is preserved.

Crash windows and their recovery:

| window | disk state | recovery |
| --- | --- | --- |
| during pass 2 | old `B.log`/`B.index` + orphan temps | orphan cleanup deletes temps (§7) |
| after temp fsync, before renames | same | same |
| after data rename, before index rename | new `B.log` + old `B.index` | hardened `restoreFromIndex` detects mismatch and rebuilds the index from the data |
| after both renames, before swap | new `B.log` + new `B.index`, fully fsynced | recovery restores the new compacted state directly |
| during swap | any of the above | as above |

## 7. Recovery of interrupted compaction

`recovery.Run` gains two responsibilities (both deterministic):

1. **Orphan cleanup.** Before scanning, remove any file in the partition dir
   whose name starts with `.tmp-` (covers both compaction temps and the existing
   transient index temps from `filesystem.AtomicWriteFile`). Deleting a temp is
   always safe: nothing is ever renamed before the file is complete and fsynced.
2. **Sparse-aware, base-derived recovery.** Sealed segments must no longer be
   assumed contiguous:
   - Parse every segment's base from its file name up front, so each sealed
     segment's LEO is `nextBase` (the following segment's base), independent of
     its data. The active segment's LEO comes from the snapshot or from its scan
     (the active is never compacted, so its scan stays contiguous).
   - The scan accepts a batch whose base offset is `>=` the previous batch's end
     (holes allowed) and `<` the segment's LEO, and advances the expected offset
     from the last decoded record's `Offset + 1` instead of `base + recordCount`.
     CRC, length, and monotonic checks still catch torn/corrupt sealed data.
   - `firstBatchMatches` accepts a first batch whose base offset is `>=` the
     segment base (a compacted segment may no longer start with its base).
3. **Hardened index trust.** `restoreFromIndex` currently spot-checks only the
   first batch. Add a per-entry header check (read the 16-byte header at each
   entry's position and compare `header[8:16]` to `base + relativeOffset`). Any
   mismatch falls back to `restoreByScan`, which rebuilds the index from the
   data. This is what heals the crash-between-renames window above and also
   hardens the existing snapshot fast path against stale index files.

## 8. Interaction with retention, snapshots

- **Retention.** Compaction and retention are driven by the **same** broker
  maintenance loop, never concurrently, so a segment cannot be closed/deleted
  mid-compaction. The engine additionally re-checks under the writer lock that
  the candidate is still in `l.segments` before the swap. Trim's age check
  (`LastTimestamp`) reads the compacted segment's surviving batches, which is
  correct: dropped records are older duplicates, so the newest surviving
  timestamp is the segment's true newest.
- **Snapshots.** The snapshot records `LEO/ActiveBase/ActiveSize/ActiveNext`, all
  of which are untouched by compaction. Sealed segment indexes change, but the
  snapshot path restores sealed segments from their index files with the
  hardened validation above, so **no snapshot format change is required**. The
  snapshot is not rewritten after a compaction (its content is identical).

## 9. Configuration

`internal/infrastructure/config` `Storage` gains:

```yaml
storage:
  compaction-interval: 30s            # maintenance cadence; 0 disables
  compaction-tombstone-retention: 24h # how long tombstones survive
  compaction-min-gain-ratio: 0.1      # skip rewrites below this shrink
```

These flow to `log.Config` (`TombstoneRetention`, `MinCompactGain`) and to
`BrokerOptions.CompactionInterval`. Engine constants default the remaining
tunables (`MaxCompactionKeys`, `MaxSegmentsPerRun`, `CompactBatchRecords`,
`CompactBatchBytes`). `topic.Config.Cleanup` already carries `delete`/`compact`;
no topic schema change is needed — enforcement is the change.

## 10. Testing strategy

- **codec**: sparse-flag round-trips, null-value round-trip, legacy contiguous
  batches unchanged, `offDelta` bounds per flag.
- **compactor unit**: dedupe within one segment; newest-wins across segments;
  keyless preserved; tombstones (retained in window / older values suppressed /
  expired dropped); fully-superseded segment deleted; gain gate; active never
  rewritten; holes visible to `Read`.
- **recovery**: sparse sealed segment full-scan recovery; sealed LEO derived from
  file names; hardened `restoreFromIndex` mismatch fallback; orphan temp cleanup;
  snapshot still valid after compaction.
- **integration / crash-at-any-point**: extend the existing reference-model
  crash test so a compacted topic's model tracks `key → latest` plus the ordered
  list of survivors and keyless records, and asserts offsets never renumber and
  payloads/timestamps of survivors are preserved. Run against concurrent
  publishes.
- **concurrency**: publish while compacting (offsets contiguous, no torn reads);
  concurrent `Compact` calls serialize.

## 11. Benchmarks

Go benchmark suite with documented baselines:

- `BenchmarkCompactThroughput` / `BenchmarkCompactAllocs` — bytes and time to
  compact a segment at various duplication ratios.
- `BenchmarkRecoverAfterCompaction` — full-scan and snapshot recovery over
  compacted segments.
- `BenchmarkLookupAfterCompaction` — `Read` from a hole and from a survivor.
- `BenchmarkAppendBeforeAfterCompaction` — publish latency unchanged while a
  compaction runs (writer lock is only held for the short swap).

## 12. Implementation order (logical commits)

1. `feat(codec): sparse-offset batches and null-value tombstones`
2. `feat(recovery): sparse recovery, base-derived LEO, hardened index, orphan cleanup`
3. `feat(engine): log compaction pass with copy-and-swap`
4. `feat(broker): maintenance loop and compaction config`
5. `feat(integration): crash-at-any-point compaction coverage`
6. `bench(engine): compaction benchmarks`
7. `docs(storage): compaction algorithm, semantics, and recovery`

Each commit compiles, passes the full gate (`gofmt`, `go vet`,
`golangci-lint`, `go test ./...`, plus `-race` and `-bench` on CI), and keeps
the docs in sync.
