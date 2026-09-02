// Compaction: a crash-safe pass that deduplicates the sealed segments of a
// compacted log, keeping only the newest record per key. See
// docs/compaction-design.md for the full design; this file implements
// sections 5 (algorithm) and 6 (copy-and-swap commit).
package log

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/storage"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/index"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/segment"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/filesystem"
)

// Engine-internal compaction tunables. These are not exposed through Config:
// docs/compaction-design.md sec 9 defaults them here rather than making them
// configurable.
const (
	// DefaultTombstoneRetention is how long a tombstone survives compaction
	// when Config.TombstoneRetention is left zero.
	DefaultTombstoneRetention = 24 * time.Hour
	// DefaultMinCompactGain is the minimum shrink ratio a rewrite must achieve
	// when Config.MinCompactGain is left zero.
	DefaultMinCompactGain = 0.1
	// MaxCompactionKeys bounds the dedupe map built by pass 1. A range that
	// would overflow it aborts the run; a later run retries.
	MaxCompactionKeys = 1 << 18
	// MaxSegmentsPerRun bounds how many sealed segments one Compact call
	// commits (rewrites or deletes), keeping each call short.
	MaxSegmentsPerRun = 4
	// CompactBatchRecords bounds the record count of a freshly re-encoded
	// batch.
	CompactBatchRecords = 1000
	// CompactBatchBytes bounds the payload byte count of a freshly re-encoded
	// batch.
	CompactBatchBytes = 1 << 20
)

// errCompactionKeyCapExceeded aborts a Compact call when pass 1's dedupe map
// would grow past MaxCompactionKeys.
var errCompactionKeyCapExceeded = errors.New("compaction key cap exceeded")

// errSegmentGone means a candidate segment was no longer present (or had
// become the active segment) by the time the swap tried to commit it —
// harmless, since retention and compaction share one maintenance loop and
// never run concurrently; this is only the engine's own defensive re-check.
var errSegmentGone = errors.New("segment no longer present for compaction swap")

// keyState is the newest occurrence of a key found by pass 1.
type keyState struct {
	offset    offset.Offset
	tombstone bool
	ts        time.Time
}

// Compact deduplicates the sealed segments of the log, keeping only the
// newest record per key. It never touches the active segment or changes any
// offset. Concurrent calls serialize on the log's compacting flag; a call
// that finds one already in progress returns a zero result.
func (l *Log) Compact(ctx context.Context) (storage.CompactionResult, error) {
	return l.compactAt(ctx, time.Now())
}

// compactAt is Compact with an injectable clock, so tombstone-retention
// behavior is deterministic in tests.
func (l *Log) compactAt(ctx context.Context, now time.Time) (storage.CompactionResult, error) {
	if err := ctx.Err(); err != nil {
		return storage.CompactionResult{}, err
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return storage.CompactionResult{}, ErrClosed
	}
	if l.compacting {
		l.mu.Unlock()
		return storage.CompactionResult{}, nil
	}
	sealed := append([]*segment.Segment(nil), l.segments[:len(l.segments)-1]...)
	active := l.active
	activeSize := active.Size()
	if len(sealed) == 0 {
		l.mu.Unlock()
		return storage.CompactionResult{}, nil
	}
	l.compacting = true
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.compacting = false
		l.mu.Unlock()
	}()

	dedupe, err := buildDedupeMap(ctx, sealed, active, activeSize)
	if err != nil {
		if errors.Is(err, errCompactionKeyCapExceeded) {
			return storage.CompactionResult{}, nil
		}
		return storage.CompactionResult{}, err
	}

	var res storage.CompactionResult
	for _, seg := range sealed {
		if res.Segments >= MaxSegmentsPerRun {
			break
		}
		if err := ctx.Err(); err != nil {
			return res, err
		}
		committed, err := l.compactOneSegment(seg, dedupe, now, &res)
		if err != nil {
			return res, err
		}
		_ = committed
	}
	return res, nil
}

// compactOneSegment rewrites (or deletes) one sealed segment and folds its
// outcome into res. A segment skipped by the gain gate, or one that vanished
// before the swap could commit, leaves res untouched.
func (l *Log) compactOneSegment(seg *segment.Segment, dedupe map[string]keyState, now time.Time, res *storage.CompactionResult) (bool, error) {
	origSize := seg.Size()
	rr, err := rewriteSegment(seg, dedupe, l.cfg, now, l.dir)
	if err != nil {
		return false, fmt.Errorf("compact segment %v: %w", seg.Base(), err)
	}

	if rr.empty {
		if err := l.deleteCompactedSegment(seg); err != nil {
			if errors.Is(err, errSegmentGone) {
				return false, nil
			}
			return false, err
		}
		res.Segments++
		res.BytesBefore += origSize
		res.TombstonesRemoved += rr.tombstonesRemoved
		return true, nil
	}

	gain := 1 - float64(rr.bytesAfter)/float64(origSize)
	if gain < l.cfg.MinCompactGain && rr.tombstonesRemoved == 0 {
		_ = os.Remove(rr.tempDataPath)
		_ = os.Remove(rr.tempIndexPath)
		return false, nil
	}

	if err := l.commitCompactedSegment(seg, rr); err != nil {
		_ = os.Remove(rr.tempDataPath)
		_ = os.Remove(rr.tempIndexPath)
		if errors.Is(err, errSegmentGone) {
			return false, nil
		}
		return false, err
	}
	res.Segments++
	res.BytesBefore += origSize
	res.BytesAfter += rr.bytesAfter
	res.TombstonesRemoved += rr.tombstonesRemoved
	return true, nil
}

// buildDedupeMap streams every sealed segment oldest-to-newest, then the
// durable prefix of the active segment (positions [0, activeSize), a
// best-effort scan: appends only grow the file, so this prefix is immutable,
// and a torn batch at the tail simply stops the scan). For every keyed
// record it keeps the highest offset seen; keyless records are ignored, since
// they are never deduplicated.
func buildDedupeMap(ctx context.Context, sealed []*segment.Segment, active *segment.Segment, activeSize int64) (map[string]keyState, error) {
	dedupe := make(map[string]keyState)
	observe := func(batch *message.RecordBatch) error {
		for i := range batch.Records {
			r := &batch.Records[i]
			if r.Message.Key == "" {
				continue
			}
			cur, ok := dedupe[r.Message.Key]
			if !ok && len(dedupe) >= MaxCompactionKeys {
				return errCompactionKeyCapExceeded
			}
			if !ok || r.Offset > cur.offset {
				dedupe[r.Message.Key] = keyState{
					offset:    r.Offset,
					tombstone: r.Message.Payload == nil,
					ts:        r.Timestamp,
				}
			}
		}
		return nil
	}

	for _, seg := range sealed {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		err := seg.ScanBatches(0, func(_, _ int64, batch *message.RecordBatch) error {
			return observe(batch)
		})
		if err != nil {
			return nil, err
		}
	}

	// Best-effort: any error (including a torn tail) just ends the prefix
	// scan early, per docs/compaction-design.md sec 5, except a genuine key
	// cap overflow, which must abort the whole run.
	err := active.ScanRange(0, activeSize, func(_, _ int64, batch *message.RecordBatch) error {
		return observe(batch)
	})
	if errors.Is(err, errCompactionKeyCapExceeded) {
		return nil, err
	}
	return dedupe, nil
}

// compactedSegment carries the outcome of rewriting one sealed segment: either
// it is empty (every record dropped, the segment should be deleted) or it has
// fresh temp data and index files ready to be swapped in.
type compactedSegment struct {
	empty             bool
	tempDataPath      string
	tempIndexPath     string
	entries           []index.Entry
	bytesAfter        int64
	tombstonesRemoved int
}

// rewriteSegment streams seg's batches, drops superseded and expired-tombstone
// records per dedupe, and re-encodes survivors into fresh batches bounded by
// CompactBatchRecords/CompactBatchBytes. The result's temp files are written
// and fsynced but not yet renamed into place.
func rewriteSegment(seg *segment.Segment, dedupe map[string]keyState, cfg Config, now time.Time, dir string) (*compactedSegment, error) {
	base := seg.Base()
	var buf []byte
	var entries []index.Entry
	nextIndexAt := int64(0)
	tombstonesRemoved := 0

	var pending []message.Record
	pendingBytes := 0
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		b := &message.RecordBatch{
			BaseOffset:     pending[0].Offset,
			FirstTimestamp: pending[0].Timestamp,
			LastTimestamp:  pending[len(pending)-1].Timestamp,
			Records:        pending,
		}
		data, err := codec.EncodeBatch(b)
		if err != nil {
			return err
		}
		pos := int64(len(buf))
		if pos >= nextIndexAt {
			entries = append(entries, index.Entry{
				RelativeOffset:   uint32(b.BaseOffset - base),
				RelativePosition: uint32(pos),
			})
			nextIndexAt = pos + cfg.IndexInterval
		}
		buf = append(buf, data...)
		pending = nil
		pendingBytes = 0
		return nil
	}

	scanErr := seg.ScanBatches(0, func(_, _ int64, batch *message.RecordBatch) error {
		for i := range batch.Records {
			r := batch.Records[i]
			if keep, tombstoneDropped := decideSurvivor(r, dedupe, cfg.TombstoneRetention, now); keep {
				pending = append(pending, r)
				pendingBytes += len(r.Message.Payload)
				if len(pending) >= CompactBatchRecords || pendingBytes >= CompactBatchBytes {
					if err := flush(); err != nil {
						return err
					}
				}
			} else if tombstoneDropped {
				tombstonesRemoved++
			}
		}
		return nil
	})
	if scanErr != nil {
		return nil, scanErr
	}
	if err := flush(); err != nil {
		return nil, err
	}

	if len(buf) == 0 {
		return &compactedSegment{empty: true, tombstonesRemoved: tombstonesRemoved}, nil
	}

	tempData := filepath.Join(dir, fmt.Sprintf(".tmp-compact-%s-%d", filesystem.SegmentName(base), time.Now().UnixNano()))
	if err := writeSyncedFile(tempData, buf); err != nil {
		return nil, err
	}
	tempIndex := tempData + ".index"
	ix := index.New(base)
	for _, e := range entries {
		if err := ix.Append(e.RelativeOffset, e.RelativePosition); err != nil {
			_ = os.Remove(tempData)
			return nil, err
		}
	}
	if err := writeSyncedFile(tempIndex, ix.Encode()); err != nil {
		_ = os.Remove(tempData)
		return nil, err
	}

	return &compactedSegment{
		tempDataPath:      tempData,
		tempIndexPath:     tempIndex,
		entries:           entries,
		bytesAfter:        int64(len(buf)),
		tombstonesRemoved: tombstonesRemoved,
	}, nil
}

// decideSurvivor applies docs/compaction-design.md sec 5 step 2's rule to one
// record: keyless records are always kept; a keyed record is kept only if it
// is the dedupe map's newest occurrence for its key (and, when that newest
// occurrence is a tombstone, only while younger than retention). tombstone
// reports whether this record was dropped specifically as an expired
// tombstone, for the caller's TombstonesRemoved count.
func decideSurvivor(r message.Record, dedupe map[string]keyState, retention time.Duration, now time.Time) (keep, tombstone bool) {
	if r.Message.Key == "" {
		return true, false
	}
	latest, ok := dedupe[r.Message.Key]
	if !ok || r.Offset < latest.offset {
		return false, false // superseded by a newer occurrence
	}
	if latest.tombstone && now.Sub(latest.ts) >= retention {
		return false, true // this key's newest value is an expired tombstone
	}
	return true, false
}

// deleteCompactedSegment removes a sealed segment whose every record was
// dropped, exactly like a retention trim of that one segment.
func (l *Log) deleteCompactedSegment(seg *segment.Segment) error {
	l.scanMu.Lock()
	defer l.scanMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	idx := findSegment(l.segments, seg)
	if idx < 0 || l.segments[idx] == l.active {
		return errSegmentGone
	}
	if err := seg.Close(); err != nil {
		return err
	}
	if err := os.Remove(seg.Path()); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(filesystem.SegmentIndexPath(l.dir, seg.Base())); err != nil && !os.IsNotExist(err) {
		return err
	}
	l.segments = append(l.segments[:idx], l.segments[idx+1:]...)
	return nil
}

// commitCompactedSegment performs the copy-and-swap commit
// (docs/compaction-design.md sec 6): under scanMu (excluding any in-flight
// read scan of this segment) and then the writer lock, it re-verifies the
// segment is still a sealed member of the log, closes it, renames the fresh
// temp data and index files over the originals, fsyncs the directory, and
// swaps in a newly opened segment recovered from the known entries and LEO.
// LEO, nextOffset, and every other segment are untouched.
func (l *Log) commitCompactedSegment(seg *segment.Segment, rr *compactedSegment) error {
	dataPath := seg.Path()
	indexPath := filesystem.SegmentIndexPath(l.dir, seg.Base())
	base := seg.Base()

	l.scanMu.Lock()
	defer l.scanMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	idx := findSegment(l.segments, seg)
	if idx < 0 || l.segments[idx] == l.active {
		return errSegmentGone
	}
	leo := seg.NextOffset()

	if err := seg.Close(); err != nil {
		return err
	}
	if err := os.Rename(rr.tempDataPath, dataPath); err != nil {
		return err
	}
	if err := os.Rename(rr.tempIndexPath, indexPath); err != nil {
		return err
	}
	if err := filesystem.SyncDir(l.dir); err != nil {
		return err
	}

	newSeg, err := segment.Open(dataPath, base, l.cfg.IndexInterval)
	if err != nil {
		return err
	}
	if err := newSeg.RecoverFrom(rr.bytesAfter, leo, rr.entries); err != nil {
		_ = newSeg.Close()
		return err
	}
	l.segments[idx] = newSeg
	return nil
}

// findSegment returns the index of seg in segments by pointer identity, or -1.
func findSegment(segments []*segment.Segment, seg *segment.Segment) int {
	for i, s := range segments {
		if s == seg {
			return i
		}
	}
	return -1
}

// writeSyncedFile creates path (which must not already exist), writes data,
// fsyncs, and closes it.
func writeSyncedFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
