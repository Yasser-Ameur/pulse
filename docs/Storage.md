# Storage

Pulse's data plane is an append-only, immutable, checksummed log divided into
segment files — the same family of design as Kafka and Redpanda, sized and
documented for a solo maintainer. This document is the storage contract:
the format, the layout, and the recovery guarantees. Phase 1 implements
append, sequential/random read, indexing, rotation, fsync, and crash recovery;
Phase 2 adds the retention sweeper and recovery snapshots. Compaction and
memory-mapping are specified here and implemented in later phases.

## 1. Guarantees

1. **An acknowledged message is never lost.** A publish is acknowledged only
   after the batch is durable (fsynced per the sync policy, which defaults to
   every write).
2. **Offsets are immutable and never reused.** A record's offset identifies it
   forever, even if it is later compacted or deleted.
3. **Ordering within a partition is total.** Records within a batch are
   contiguous; batches are written under a single writer lock.
4. **Torn writes are recoverable.** A crash mid-write leaves at most the tail
   batch damaged; recovery truncates at the last valid batch.

## 2. Layout

All files live under the configured data directory.

```
data/
├── meta/                             # Pebble metadata store (see §6)
└── topics/
    └── <topic>/
        └── <partition>/
            ├── 00000000000000000000.log      # data segment
            ├── 00000000000000000000.index    # offset index for the segment
            ├── 00000000000000000010.log      # next segment (rotation)
            ├── 00000000000000000010.index
            └── snapshot                      # recovery checkpoint (see §8)
```

- Segment files are named by their base offset, zero-padded to 20 digits, with
  a `.log` extension; indexes share the name with `.index`.
- A segment spans `[baseOffset, nextBaseOffset)` where `nextBaseOffset` is the
  base offset of the following segment (or the log's LEO for the active one).
- `<topic>` is the validated topic name; `<partition>` is the partition id.
  Topic names may not contain path separators (enforced by validation).

## 3. Batch format

Data is written as **record batches**. Batching groups several messages into
one durable frame, which keeps fsync and I/O costs flat as batch size grows and
provides the natural unit for future compression.

All integers are big-endian. One batch:

```
offset  size  field
0       1     magic = 0x01
1       1     version = 0x01
2       2     flags            (bit 0..2 reserved for compression codec; 0 today)
4       4     crc32c           (Castagnoli, over bytes from baseOffset to end of records)
8       8     baseOffset       (int64, first offset in the batch)
16      4     batchLen         (uint32, byte length of the records section)
20      8     firstTimestamp   (int64, millis; first record)
28      8     lastTimestamp    (int64, millis; last record)
36      4     recordCount      (uint32)
40      4     producerID       (uint32; 0 = unset, reserved for exactly-once)
44      4     producerEpoch    (uint32; 0 = unset, reserved)
48      4     baseSequence     (int32; 0 = unset, reserved)
52      n     records          (recordCount records, each self-delimiting)
```

Header size is 52 bytes for version 0x01. Fields `producerID`, `producerEpoch`,
`baseSequence` and the compression bits of `flags` are reserved: their presence
makes the format forward-compatible with exactly-once and compression without a
format bump.

### Record

```
field             size   note
length            uint32 byte length of everything that follows
attributes        uint8  reserved (0)
timestampDelta    int32  millis since batch firstTimestamp
offsetDelta       uint32 offset = baseOffset + offsetDelta
keyLength         uint32 0xFFFFFFFF = null key
key               keyLength bytes
valueLength       uint32 0xFFFFFFFF = null value
value             valueLength bytes
headerCount       uint32
headers           headerCount × (keyLen uint32, key, valueLen uint32, value)
```

The `offsetDelta` is checked against `recordCount` during decode so that a
corrupt batch cannot fabricate offsets outside its own range.

### CRC32C

The CRC is computed over the entire batch from `baseOffset` through the end of
the records section using CRC-32C (Castagnoli) — the standard for checksummed
framing on modern hardware. Decode verifies the CRC before trusting any record.

## 4. Index format

Each segment has a **sparse** offset index: one fixed-size entry per ~4 KiB of
data appended (configurable `storage.index-interval-bytes`). The index trades a
little read-ahead for a tiny, cacheable file.

```
entry = relativeOffset (uint32) | relativePosition (uint32)
entry size = 8 bytes, big-endian
```

- `relativeOffset` is the batch's base offset minus the segment's base offset.
- `relativePosition` is the batch's byte position within the segment file.
- Entries are written in strictly increasing order.

Lookup for offset `O`: binary search the index for the greatest entry with
`relativeOffset <= O - segmentBase`, seek the segment file to
`relativePosition`, then decode forward until the batch containing `O`.

Both fields are 32-bit because relative offsets and positions are bounded by a
single segment's size; rotation (below) keeps segments well under 2 GiB. The
format reserves headroom to widen this if segments ever approach 2 GiB.

## 5. Rotation and sync

- The active segment rolls to a new file when its byte size exceeds
  `storage.segment-max-bytes` (default 512 MiB) or when a single batch would not
  fit. The new segment's base offset is the current LEO.
- Rotation is performed under the log's writer lock so it is atomic with
  respect to appends.
- `storage.sync-mode` controls durability:
  - `every-write` (default): fsync after each batch; publish acks are strict.
  - `interval`: fsync at most once per configured interval; acks are slightly
    lax (documented trade-off), used for throughput.
- Index entries are flushed with the data file on fsync.

## 6. Metadata plane

The metadata store (Pebble in Phase 1) holds broker state only. Keys are
namespaced:

```
meta/schema-version          → int64
cluster/current              → id of the active cluster (pointer)
cluster/<id>                 → cluster identity record
broker/current               → id of the active broker (pointer)
broker/<id>                  → broker identity record
topic/<name>                 → serialized topic definition (name, config, created_at)
partition/<topic>/<id>       → serialized partition metadata
cursor/<consumer>/<topic>/<p> → consumer cursor (int64 offset)
```

Identity creation writes both the `cluster/<id>` / `broker/<id>` record and the
`current` pointer, so `ClusterID` / `BrokerID` resolve in a single lookup.

Writes that must be durable (topic create/delete, cursor updates, identity
creation) use Pebble sync commits. Event data never passes through the
metadata store.

## 7. Recovery

On startup, for every partition directory the engine performs **log
recovery**: the snapshot fast path (§8) when a valid checkpoint matches the
on-disk state, otherwise the full scan below. The scan is the correctness
baseline — correctness never depends on a snapshot being present.

The full scan proceeds as follows:

1. Enumerate `.log` files in offset order; load index files.
2. For each sealed segment, validate its index is consistent with the data
   (positions within file, offsets monotonic); rebuild the index from the data
   if the index file is missing or shorter than the data.
3. For the active (last) segment, scan batches from the last index entry:
   - if a batch header overruns the file end, the file is truncated at the
     previous batch boundary;
   - if a batch CRC fails, the file is truncated at the previous batch boundary
     and the corruption is logged;
   - otherwise the batch is accepted and LEO advances.
4. Rebuild the active segment's index tail if truncation occurred.
5. Recompute LEO and high watermark from the final offsets.

A torn tail is therefore always truncated to the last fully-written, CRC-valid
batch — never partially accepted. This is what makes guarantee #1 hold across
crashes.

## 8. Retention, snapshots, compaction

### Time/size retention (implemented)

A background sweeper (period `storage.retention-interval`, default 30s, disabled
when zero) applies each topic's `retention` policy to its partition logs. Trim
runs under the log's writer lock and deletes whole **sealed** segments from
oldest to newest; the active segment is never deleted because it receives new
data.

- **Time**: a sealed segment is deleted when its newest record is older than
  `retention.max-age` at sweep time.
- **Size**: sealed segments are deleted until the total log size (including the
  active segment) is within `retention.max-bytes`.
- Both limits are independent and can be combined; a zero limit disables that
  axis.

Trimming preserves the address space: surviving offsets never change, and a
consumer reading from below the trimmed prefix simply begins at the oldest
surviving record. The durable snapshot (below) is rewritten after a trim, so a
restart trusts the trimmed state. Deletion is not cursor-aware today: a slow
consumer behind the trimmed prefix misses those records. Protecting the segment
containing any live consumer cursor is planned.

### Snapshots (implemented)

Every partition has one `snapshot` file — a tiny, atomically-written checkpoint
that makes recovery constant-time instead of scan-from-epoch:

```
offset  size  field
0       1     magic = 0x53 ('S')
1       1     version = 0x01
2       2     reserved
4       4     crc32c (Castagnoli, over bytes 8..40)
8       8     leo        (int64, log end offset)
16      8     activeBase (int64, base offset of the active segment)
24      8     activeSize (int64, valid byte length of the active segment)
32      8     activeNext (int64, active segment next offset; equals leo)
```

The file is written to a temp file, fsynced, and renamed into place, so a crash
leaves either the old or the new checkpoint, never a torn mix. It is rewritten
after each rotation, after a trim, and on clean shutdown.

At startup recovery prefers the snapshot:

1. If it is missing, corrupt, or describes a different active segment than is on
   disk, it is discarded and the full scan in §7 runs.
2. Sealed segments are restored from their persisted index files after a cheap
   spot-check of the first batch's base offset against the segment name; a
   missing or corrupt index falls back to scanning that segment.
3. If the active segment matches `activeSize` exactly, it is trusted. If it grew
   past the checkpoint, only the delta tail is scanned and truncated at the last
   valid batch.

Because the checkpoint always records a batch boundary, the trusted prefix is
never cut mid-batch, and a torn tail after a valid checkpoint is handled without
a full scan. Sealed-segment corruption is only detected when the data is read,
not at startup.

### Compaction (future)

For compacted topics, a background pass rewrites segments keeping only the last
record per key, then atomically swaps the old segments for new ones and truncates
the corresponding index. The log address space is preserved: offsets of surviving
records do not change.

### Memory mapping (future)

Read-only `mmap` of sealed segments to enable zero-copy reads; the active segment
stays buffered. Deferred because the sequential read path is already
allocation-friendly.

## 9. Why this format (decisions and rationale)

- **Batches, not single records**: fsync and append are per-batch, so a batch of
  N messages costs roughly the same as one; this is the primary lever for
  high-throughput publish.
- **Sparse index**: a 512 MiB segment needs ~131k entries (~1 MiB) at 4 KiB
  intervals — cheap to keep resident — and lookup needs at most a few KiB of
  decode. Dense indexing would be pure waste.
- **CRC32C over the whole batch**: catches torn writes and bit rot at the frame
  level before any record is trusted; per-record CRCs would add overhead with
  little benefit given the frame already covers them.
- **Big-endian and fixed-width headers**: deterministic bytes make the format
  debuggable with a hex dump and portable across platforms.
- **20-digit base offsets**: lexicographic sort order == offset order, so
  directory listing and glob patterns never need a numeric sort.

## 10. Filesystem responsibilities

`infrastructure/storage/filesystem` owns: path construction, safe file open
flags (O_APPEND|O_CREATE, no O_TRUNC for data files), segment naming, and the
few durability helpers (fsync, atomic index rebuild via temp file + rename).
All other storage packages treat files as abstract byte streams.
