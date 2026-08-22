package log

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/retention"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

// contentionCfg keeps every record in one segment and takes fsync off the
// append path, so a measured append latency is lock wait plus one WriteAt and
// nothing else. Absolute durations on any given machine are meaningless here;
// only the ratios below are.
func contentionCfg() Config {
	return Config{
		MaxSegmentBytes: 1 << 30,
		IndexInterval:   4096,
		SyncMode:        SyncInterval,
		SyncInterval:    time.Hour,
	}
}

func newContentionLog(t *testing.T, batches int) *Log {
	t.Helper()
	root := t.TempDir()
	name, _ := topic.NewName("orders")
	l, err := CreateLog(root, name, partition.ID(0), contentionCfg(), nil)
	if err != nil {
		t.Fatalf("CreateLog() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	for i := 0; i < batches; i++ {
		appendOne(t, l)
	}
	return l
}

func appendOne(t *testing.T, l *Log) {
	t.Helper()
	now := time.Unix(1700000000, 0).UTC()
	recs := make([]message.Record, 10)
	for i := range recs {
		recs[i] = message.Record{Timestamp: now, Message: message.Message{Payload: make([]byte, 100)}}
	}
	if _, err := l.Append(context.Background(), &message.RecordBatch{
		FirstTimestamp: now,
		LastTimestamp:  now,
		Records:        recs,
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

// appendUnderScan starts an unbounded Read of scan, waits until that scan is
// genuinely running, then appends one batch to pub. It returns how long the
// append took and how long the scan took.
//
// Comparing those two is the whole measurement, and it is why nothing here
// depends on how fast or how quiet the machine is: the scan is the yardstick
// the append is measured against, and both are stretched by the same machine.
// An append that takes as long as the scan waited for the scan. One that takes
// a small fraction of it did not.
func appendUnderScan(t *testing.T, pub, scan *Log) (appended, scanned time.Duration) {
	t.Helper()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		s := time.Now()
		_, _ = scan.Read(context.Background(), offset.Offset(0), 0, 0)
		scanned = time.Since(s)
		close(done)
	}()
	<-started
	time.Sleep(2 * time.Millisecond)
	s := time.Now()
	appendOne(t, pub)
	appended = time.Since(s)
	<-done
	return appended, scanned
}

// TestAppendDoesNotWaitOnReadScan asserts that an in-flight Read does not delay
// a concurrent Append, and does it in a way that cannot be satisfied by a quiet
// machine or defeated by a busy one.
//
// Two arms run interleaved in one process against the same publisher:
//
//	same  — append while an unbounded Read scans the log being appended to.
//	decoy — append while an identical unbounded Read scans a DIFFERENT log of
//	        the same size: same syscalls, same decode work, same page-cache and
//	        antivirus pressure, no shared lock.
//
// Each arm's append is scored as a fraction of its own scan's duration, so the
// result is a pure ratio and a slow machine stretches both terms together. The
// decoy arm prices everything a scan costs except the lock; whatever separates
// the two arms is the lock and nothing else.
//
// When Read held the log's read lock across its decode, the same arm measured
// 97.5%-98.7% of its scan against the decoy arm's 0.0%-1.8%: the publish waited
// out the whole scan. The threshold below is set far above what a correct
// implementation produces and far below that, so it catches the regression
// without depending on the machine.
func TestAppendDoesNotWaitOnReadScan(t *testing.T) {
	const (
		rounds  = 5
		batches = 4000
	)
	pub := newContentionLog(t, batches)
	decoy := newContentionLog(t, batches)

	var sameWorst, decoyWorst float64
	for i := 0; i < rounds; i++ {
		da, ds := appendUnderScan(t, pub, decoy)
		sa, ss := appendUnderScan(t, pub, pub)
		dFrac, sFrac := frac(da, ds), frac(sa, ss)
		t.Logf("round %d  decoy: append %v of a %v scan (%.1f%%) | same: append %v of a %v scan (%.1f%%)",
			i, da, ds, dFrac*100, sa, ss, sFrac*100)
		if dFrac > decoyWorst {
			decoyWorst = dFrac
		}
		if sFrac > sameWorst {
			sameWorst = sFrac
		}
	}
	t.Logf("worst same-log append = %.1f%% of its scan; worst decoy append = %.1f%% of its scan",
		sameWorst*100, decoyWorst*100)

	if sameWorst > 0.25 {
		t.Errorf("append concurrent with a scan of the same log took %.1f%% of that scan, want <=25%% "+
			"(decoy arm, same work without the shared lock, took %.1f%%): the read path is blocking publishes",
			sameWorst*100, decoyWorst*100)
	}
}

// TestReadConcurrentWithWrites exercises the read path against every operation
// that mutates a segment under it, for the race detector. Read now decodes
// outside the metadata lock, so this is what proves it reads nothing a writer
// is concurrently changing.
func TestReadConcurrentWithWrites(t *testing.T) {
	// Small segments so the writer really does rotate and Trim really does
	// close and delete segments out from under the readers.
	_, l := newTestLog(t, Config{
		MaxSegmentBytes: 16 << 10,
		IndexInterval:   256,
		SyncMode:        SyncInterval,
		SyncInterval:    time.Hour,
	})
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := l.Read(context.Background(), offset.Offset(0), 0, 0); err != nil {
					t.Errorf("Read() error = %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < 300; i++ {
		appendOne(t, l)
		if i%50 == 0 {
			if _, err := l.Trim(time.Now(), retention.Policy{MaxBytes: 1 << 16}); err != nil {
				t.Errorf("Trim() error = %v", err)
				break
			}
		}
	}
	close(stop)
	wg.Wait()
}

func frac(a, b time.Duration) float64 {
	if b <= 0 {
		return 0
	}
	return float64(a) / float64(b)
}
