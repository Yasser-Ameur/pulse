// Package log implements the durable, append-only partition log (the storage
// engine's implementation of the domain storage.Log port).
//
// A log is a sequence of segments (docs/Storage.md §2, §5). Appends go
// through a single writer lock; reads take a shared lock and use positional
// file reads so concurrent subscribers never contend on a file offset. The
// sparse index is rebuilt from data on recovery, and a torn tail is truncated
// at the last CRC-valid batch.
package log

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/retention"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/codec"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/recovery"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/segment"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/snapshot"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/filesystem"
)

// Defaults applied when a Config field is left zero.
const (
	DefaultMaxSegmentBytes int64 = 512 << 20 // 512 MiB
	DefaultIndexInterval   int64 = 4096      // one index entry per 4 KiB
)

// SyncMode selects the durability policy.
type SyncMode string

const (
	// SyncEveryWrite fsyncs after every batch; publishes are strictly durable.
	SyncEveryWrite SyncMode = "every-write"
	// SyncInterval fsyncs at most once per configured interval; publishes are
	// acknowledged with slightly relaxed durability for throughput.
	SyncInterval SyncMode = "interval"
)

// Sentinel errors returned by the log.
var (
	// ErrClosed reports an operation on a closed log.
	ErrClosed = errors.New("log closed")
)

// Config carries the engine tunables for a log.
type Config struct {
	// MaxSegmentBytes rotates the active segment once its size would exceed
	// this bound (a single larger batch is still appended).
	MaxSegmentBytes int64
	// IndexInterval adds a sparse index entry every this many bytes.
	IndexInterval int64
	// SyncMode is the durability policy; empty means SyncEveryWrite.
	SyncMode SyncMode
	// SyncInterval is the fsync period for SyncInterval mode.
	SyncInterval time.Duration
}

// ApplyDefaults fills zero fields with engine defaults.
func (c Config) ApplyDefaults() Config {
	if c.MaxSegmentBytes <= 0 {
		c.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if c.IndexInterval <= 0 {
		c.IndexInterval = DefaultIndexInterval
	}
	if c.SyncMode == "" {
		c.SyncMode = SyncEveryWrite
	}
	if c.SyncMode == SyncInterval && c.SyncInterval <= 0 {
		c.SyncInterval = 100 * time.Millisecond
	}
	return c
}

// Log is one partition's durable log.
type Log struct {
	dir     string
	cfg     Config
	baseDir string
	name    topic.Name
	pid     partition.ID
	logger  ports.Logger

	mu         sync.RWMutex
	segments   []*segment.Segment
	active     *segment.Segment
	nextOffset offset.Offset
	closed     bool
	notify     chan struct{}
	stopFlush  chan struct{}
}

// OpenLog recovers the partition log at dir. If the directory does not exist
// an error wrapping ports.ErrLogNotFound is returned so the caller can
// recreate the partition from metadata.
func OpenLog(root string, name topic.Name, pid partition.ID, cfg Config, logger ports.Logger) (*Log, error) {
	cfg = cfg.ApplyDefaults()
	dir := filesystem.PartitionDir(root, name, pid)
	if !filesystem.PartitionExists(root, name, pid) {
		return nil, fmt.Errorf("%w: %s", ports.ErrLogNotFound, dir)
	}
	res, err := recovery.Run(dir, cfg.IndexInterval)
	if err != nil {
		return nil, err
	}
	l := &Log{
		dir:     dir,
		cfg:     cfg,
		baseDir: root,
		name:    name,
		pid:     pid,
		logger:  logger,
		notify:  make(chan struct{}),
	}
	if len(res.Segments) == 0 {
		seg, err := newBaseSegment(dir, 0, cfg)
		if err != nil {
			return nil, err
		}
		l.segments = []*segment.Segment{seg}
		l.active = seg
	} else {
		l.segments = res.Segments
		l.active = res.Segments[len(res.Segments)-1]
		if res.Truncated && logger != nil {
			logger.Warn("recovered torn log tail",
				ports.Field{Key: "topic", Value: name.String()},
				ports.Field{Key: "partition", Value: pid.Int32()},
				ports.Field{Key: "truncated_bytes", Value: res.TruncatedBytes},
			)
		}
	}
	l.nextOffset = l.active.NextOffset()
	if res.SnapshotUsed && logger != nil {
		logger.Info("recovered log from snapshot",
			ports.Field{Key: "topic", Value: name.String()},
			ports.Field{Key: "partition", Value: pid.Int32()},
		)
	}
	l.startFlusher()
	return l, nil
}

// CreateLog creates a new empty partition log at dir, creating directories.
func CreateLog(root string, name topic.Name, pid partition.ID, cfg Config, logger ports.Logger) (*Log, error) {
	cfg = cfg.ApplyDefaults()
	dir := filesystem.PartitionDir(root, name, pid)
	if err := filesystem.MkdirAll(dir); err != nil {
		return nil, err
	}
	seg, err := newBaseSegment(dir, 0, cfg)
	if err != nil {
		return nil, err
	}
	l := &Log{
		dir:     dir,
		cfg:     cfg,
		baseDir: root,
		name:    name,
		pid:     pid,
		logger:  logger,
		notify:  make(chan struct{}),
	}
	l.segments = []*segment.Segment{seg}
	l.active = seg
	l.startFlusher()
	return l, nil
}

// DeleteLog removes the partition directory and all its data.
func DeleteLog(root string, name topic.Name, pid partition.ID) error {
	return os.RemoveAll(filesystem.PartitionDir(root, name, pid))
}

func newBaseSegment(dir string, base offset.Offset, cfg Config) (*segment.Segment, error) {
	return segment.Open(filesystem.SegmentLogPath(dir, base), base, cfg.IndexInterval)
}

// Append encodes and durably writes batch, assigning it consecutive offsets.
func (l *Log) Append(ctx context.Context, batch *message.RecordBatch) (offset.Offset, error) {
	if err := ctx.Err(); err != nil {
		return offset.Invalid, err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return offset.Invalid, ErrClosed
	}
	base := l.nextOffset
	batch.BaseOffset = base
	for i := range batch.Records {
		batch.Records[i].Offset = base + offset.Offset(i)
	}
	data, err := codec.EncodeBatch(batch)
	if err != nil {
		l.mu.Unlock()
		return offset.Invalid, err
	}
	if l.active.Size() > 0 && l.active.Size()+int64(len(data)) > l.cfg.MaxSegmentBytes {
		if err := l.rotate(); err != nil {
			l.mu.Unlock()
			return offset.Invalid, err
		}
	}
	if err := l.active.Append(data, uint32(len(batch.Records))); err != nil {
		l.mu.Unlock()
		return offset.Invalid, err
	}
	l.nextOffset += offset.Offset(len(batch.Records))
	close(l.notify)
	l.notify = make(chan struct{})
	if l.cfg.SyncMode != SyncInterval {
		if err := l.active.Sync(); err != nil {
			l.mu.Unlock()
			return base, err
		}
	}
	l.mu.Unlock()
	return base, nil
}

// Read returns up to limit records at or after from, bounded by maxBytes of
// payload (a single record larger than the bound is still returned).
func (l *Log) Read(ctx context.Context, from offset.Offset, limit int, maxBytes int) ([]message.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !from.Valid() {
		return nil, offset.ErrInvalid
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	if from >= l.nextOffset {
		return nil, nil
	}
	segIdx := 0
	for segIdx < len(l.segments) && from >= l.segments[segIdx].NextOffset() {
		segIdx++
	}
	var out []message.Record
	payloadBytes := 0
	stop := false
	for segIdx < len(l.segments) && !stop {
		seg := l.segments[segIdx]
		startPos := int64(0)
		if from < seg.Base() {
			startPos = 0
		} else if p, ok := seg.PositionFor(from); ok {
			startPos = p
		}
		err := seg.ScanBatches(startPos, func(_ int64, _ int64, batch *message.RecordBatch) error {
			for i := range batch.Records {
				r := batch.Records[i]
				if r.Offset < from {
					continue
				}
				if limit > 0 && len(out) >= limit {
					stop = true
					return errStop
				}
				if maxBytes > 0 && len(out) > 0 && payloadBytes+len(r.Message.Payload) > maxBytes {
					stop = true
					return errStop
				}
				out = append(out, r)
				payloadBytes += len(r.Message.Payload)
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			return nil, err
		}
		segIdx++
	}
	return out, nil
}

// Trim applies the retention policy at now, deleting a prefix of sealed
// segments that fall entirely outside the retention window. The active segment
// is never deleted and offsets of surviving records never change. Both policy
// limits are applied independently; a zero limit is ignored. Safe for
// concurrent use.
func (l *Log) Trim(now time.Time, policy retention.Policy) (retention.TrimResult, error) {
	if policy.MaxAge <= 0 && policy.MaxBytes <= 0 {
		return retention.TrimResult{}, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return retention.TrimResult{}, ErrClosed
	}
	var res retention.TrimResult

	// Time-based retention: drop a prefix of sealed segments whose newest
	// record predates the cutoff. Batches are time-ordered across segments, so
	// the first segment inside the window ends the sweep.
	if policy.MaxAge > 0 {
		cutoff := now.Add(-policy.MaxAge)
		n := 0
		for n < len(l.segments)-1 {
			ts, err := l.segments[n].LastTimestamp()
			if err != nil {
				return res, err
			}
			if !ts.Before(cutoff) {
				break
			}
			n++
		}
		if err := l.dropSealed(n, &res); err != nil {
			return res, err
		}
	}

	// Size-based retention: drop the oldest sealed segments while the log's
	// total size (including the protected active segment) exceeds the budget.
	if policy.MaxBytes > 0 {
		total := int64(0)
		for _, seg := range l.segments {
			total += seg.Size()
		}
		n := 0
		for n < len(l.segments)-1 && total > policy.MaxBytes {
			total -= l.segments[n].Size()
			n++
		}
		if err := l.dropSealed(n, &res); err != nil {
			return res, err
		}
	}
	return res, nil
}

// dropSealed closes and deletes the first n sealed segments, removing them
// from the segment list and accumulating the result. The caller holds l.mu.
func (l *Log) dropSealed(n int, res *retention.TrimResult) error {
	for i := 0; i < n; i++ {
		seg := l.segments[i]
		res.Segments++
		res.Bytes += seg.Size()
		if err := seg.Close(); err != nil {
			return err
		}
		if err := os.Remove(seg.Path()); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Remove(filesystem.SegmentIndexPath(l.dir, seg.Base())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	l.segments = l.segments[n:]
	return nil
}

// NextOffset returns the offset the next append receives.
func (l *Log) NextOffset() offset.Offset {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

// Notify returns a channel closed on the next append.
func (l *Log) Notify() <-chan struct{} {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.notify
}

// Sync flushes the active segment and its index to stable storage.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.active.Sync()
}

// Truncate removes all batches whose first offset is at or after to. The log
// is returned to the state it would have had without those records. Batches
// are atomic: a truncation point inside a batch keeps the whole batch.
func (l *Log) Truncate(to offset.Offset) error {
	if !to.Valid() {
		return offset.ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if to >= l.nextOffset {
		return nil
	}
	// Find the segment containing to: the first whose LEO exceeds it.
	idx := 0
	for idx < len(l.segments) && to >= l.segments[idx].NextOffset() {
		idx++
	}
	if idx == len(l.segments) {
		return nil
	}
	seg := l.segments[idx]

	// Walk batches to find the last one whose records end at or before to.
	var validSize, lastEnd int64
	var newNext, lastNext offset.Offset
	lastEnd = 0
	lastNext = seg.Base()
	found := false
	err := seg.ScanBatches(0, func(pos, end int64, batch *message.RecordBatch) error {
		if batch.BaseOffset >= to {
			validSize, newNext = lastEnd, lastNext
			found = true
			return errStop
		}
		lastEnd = end
		lastNext = batch.BaseOffset + offset.Offset(len(batch.Records))
		return nil
	})
	if err != nil && !errors.Is(err, errStop) {
		return err
	}
	if !found {
		validSize, newNext = lastEnd, lastNext
	}
	if err := seg.TruncateTo(validSize, newNext); err != nil {
		return err
	}

	// Drop any segments after the truncation point.
	for i := idx + 1; i < len(l.segments); i++ {
		if err := l.segments[i].Close(); err != nil {
			return err
		}
		if err := os.Remove(l.segments[i].Path()); err != nil {
			return err
		}
	}
	l.segments = l.segments[:idx+1]
	l.active = seg
	l.nextOffset = newNext
	close(l.notify)
	l.notify = make(chan struct{})
	l.writeSnapshot()
	return nil
}

// Close flushes and releases all segment resources.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	if l.stopFlush != nil {
		close(l.stopFlush)
	}
	var errs []error
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	l.writeSnapshot()
	l.closed = true
	return errors.Join(errs...)
}

// rotate seals the active segment and opens the next one at the current LEO.
// The caller holds l.mu.
func (l *Log) rotate() error {
	if err := l.active.Seal(); err != nil {
		return err
	}
	base := l.nextOffset
	seg, err := segment.Open(filesystem.SegmentLogPath(l.dir, base), base, l.cfg.IndexInterval)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.active = seg
	l.writeSnapshot()
	return nil
}

// writeSnapshot durably checkpoints the log's LEO and active segment state so
// a future recovery can skip the scan-from-epoch. It is best-effort: the
// snapshot is an optimization, so a failed write only costs a slower recovery.
// The caller holds l.mu.
func (l *Log) writeSnapshot() {
	st := snapshot.State{
		LEO:        l.nextOffset,
		ActiveBase: l.active.Base(),
		ActiveSize: l.active.Size(),
		ActiveNext: l.active.NextOffset(),
	}
	if err := snapshot.Write(l.dir, st); err != nil && l.logger != nil {
		l.logger.Warn("write recovery snapshot failed",
			ports.Field{Key: "error", Value: err.Error()})
	}
}

// startFlusher runs the periodic sync for SyncInterval mode.
func (l *Log) startFlusher() {
	if l.cfg.SyncMode != SyncInterval || l.cfg.SyncInterval <= 0 {
		return
	}
	l.stopFlush = make(chan struct{})
	go func() {
		t := time.NewTicker(l.cfg.SyncInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				l.mu.Lock()
				if !l.closed {
					_ = l.active.Sync()
				}
				l.mu.Unlock()
			case <-l.stopFlush:
				return
			}
		}
	}()
}

// errStop halts a ScanBatches walk.
var errStop = errors.New("stop scan")
