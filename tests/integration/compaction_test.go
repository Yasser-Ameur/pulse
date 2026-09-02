// Log compaction, run through the real broker over gRPC: a reference-model
// equivalence check for a compacted topic against crash windows in the
// copy-and-swap commit (docs/compaction-design.md sec 6-7), and a
// publish-while-compacting concurrency check.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/testutil"
	"github.com/Yasser-Ameur/pulse/pkg/client"
	"github.com/stretchr/testify/require"
)

// pubRecord tracks one published record for the reference model.
type pubRecord struct {
	offset int64
	key    string
}

// expectedSurvivorOffsets returns the offsets that must survive compaction:
// every keyless publish, and for each key only its highest (newest) offset.
func expectedSurvivorOffsets(pubs []pubRecord) map[int64]bool {
	latestByKey := make(map[string]int64)
	for _, p := range pubs {
		if p.key == "" {
			continue
		}
		latestByKey[p.key] = p.offset // pubs is offset-ordered, so last write wins
	}
	latest := make(map[int64]bool, len(latestByKey))
	for _, o := range latestByKey {
		latest[o] = true
	}
	survivors := make(map[int64]bool)
	for _, p := range pubs {
		if p.key == "" || latest[p.offset] {
			survivors[p.offset] = true
		}
	}
	return survivors
}

// filterRecords keeps only the records whose offset is in keep, preserving
// order.
func filterRecords(all []client.Record, keep map[int64]bool) []client.Record {
	out := make([]client.Record, 0, len(keep))
	for _, r := range all {
		if keep[r.Offset] {
			out = append(out, r)
		}
	}
	return out
}

// requireRecordsEqual asserts want and got carry identical offsets, keys,
// payloads, and timestamps in the same order: exactly what "offsets never
// renumbered, survivors' payloads and timestamps preserved" means.
func requireRecordsEqual(t *testing.T, want, got []client.Record) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].Offset, got[i].Offset, "record %d offset", i)
		require.Equal(t, want[i].Message.Key, got[i].Message.Key, "record %d key", i)
		require.Equal(t, want[i].Message.Payload, got[i].Message.Payload, "record %d payload", i)
		require.True(t, want[i].Timestamp.Equal(got[i].Timestamp), "record %d timestamp %v vs %v", i, want[i].Timestamp, got[i].Timestamp)
	}
}

// plantOrphanCompactionTemps writes garbage ".tmp-compact-*" data and index
// files into the partition directory, simulating a crash during pass 2 or
// right after the temp files were fsynced but before any rename (the first
// two rows of docs/compaction-design.md sec 6's crash table). Recovery must
// delete them and leave the real segments untouched.
func plantOrphanCompactionTemps(t *testing.T, dir, name string) {
	t.Helper()
	pdir := partitionDir(dir, name, 0)
	for _, suffix := range []string{"", ".index"} {
		path := filepath.Join(pdir, fmt.Sprintf(".tmp-compact-00000000000000000000-%d%s", time.Now().UnixNano(), suffix))
		require.NoError(t, os.WriteFile(path, []byte("garbage-from-an-interrupted-compaction"), 0o644))
	}
}

// snapshotFiles reads every file in the partition directory whose name ends
// in suffix, keyed by file name.
func snapshotFiles(t *testing.T, dir, name, suffix string) map[string][]byte {
	t.Helper()
	pdir := partitionDir(dir, name, 0)
	entries, err := os.ReadDir(pdir)
	require.NoError(t, err)
	out := make(map[string][]byte)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), suffix) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(pdir, e.Name()))
		require.NoError(t, err)
		out[e.Name()] = b
	}
	return out
}

// restoreChangedFiles writes back pre's bytes for every file that compaction
// actually changed, simulating a crash after the data rename but before the
// index rename (docs/compaction-design.md sec 6's third crash row): the data
// ends up fully compacted while its index is left describing the old layout.
// A file present in pre but missing now (a fully-superseded segment that got
// deleted) is left alone — there is nothing to revert it to.
func restoreChangedFiles(t *testing.T, dir, name string, pre map[string][]byte) {
	t.Helper()
	pdir := partitionDir(dir, name, 0)
	for fname, oldBytes := range pre {
		path := filepath.Join(pdir, fname)
		cur, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if !bytes.Equal(cur, oldBytes) {
			require.NoError(t, os.WriteFile(path, oldBytes, 0o644))
		}
	}
}

// sweepToConvergence runs the broker's real sweeper repeatedly until a call
// compacts nothing further, bounded by maxRounds. One call to Log.Compact
// only rewrites up to MaxSegmentsPerRun sealed segments, so a topic with many
// small segments (as a tiny MaxSegmentBytes in a test produces) needs several
// sweeps to fully converge — exactly as it would over several ticks of the
// real background sweeper.
func sweepToConvergence(t *testing.T, inst *testutil.Instance, maxRounds int) {
	t.Helper()
	for i := 0; i < maxRounds; i++ {
		inst.Broker().Sweep()
	}
}

// TestCompactionCrashAtAnyPointRecovery is the reference-model equivalence
// test docs/compaction-design.md sec 10 requires: it drives a compacted
// topic's dedup through the broker's real sweeper (Log.Compact via
// Broker.Sweep), and injects a simulated crash at each documented window of
// the copy-and-swap commit — before any rename, and between the data and
// index renames — verifying every time that keys map to their latest value,
// keyless records and their offsets are untouched, offsets are never
// renumbered, and survivors' payloads and timestamps are exactly preserved.
func TestCompactionCrashAtAnyPointRecovery(t *testing.T) {
	dir := t.TempDir()
	const name = "compacted"
	ctx := context.Background()
	// A tiny segment size forces frequent rotation, so a short run produces
	// several sealed segments for compaction to work on.
	startOpts := func() testutil.Options {
		return testutil.Options{Dir: dir, IndexInterval: 32, MaxSegmentBytes: 120}
	}

	inst := testutil.Start(t, startOpts())
	c := dial(t, inst)
	_, err := c.CreateTopic(ctx, name, client.TopicConfig{Partitions: 1, Cleanup: "compact"})
	require.NoError(t, err)

	keys := []string{"a", "b", "c", ""} // "" is keyless
	var pubs []pubRecord
	for i := 0; i < 28; i++ {
		key := keys[i%len(keys)]
		offs, err := c.Publish(ctx, name, 0, client.Message{
			Key:         key,
			Payload:     []byte(fmt.Sprintf("v-%d", i)),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
		require.Len(t, offs, 1)
		pubs = append(pubs, pubRecord{offset: offs[0], key: key})
	}

	initial := consume(t, c, name, 0, client.SubscribeOptions{})
	require.Len(t, initial, len(pubs))
	survivors := expectedSurvivorOffsets(pubs)
	expected := filterRecords(initial, survivors)
	require.Less(t, len(expected), len(initial), "the key set must produce real duplicates to drop")

	// --- Window 1: crash before any rename ever happens. ---
	// Nothing has been compacted yet, so the log must recover exactly as
	// published; the planted orphans must be gone.
	plantOrphanCompactionTemps(t, dir, name)
	inst.Stop(t)
	inst = testutil.Start(t, startOpts())
	c = dial(t, inst)
	requireRecordsEqual(t, initial, consume(t, c, name, 0, client.SubscribeOptions{}))
	requireNoOrphanTemps(t, dir, name)

	// --- Real compaction, run through the sweeper's actual path. ---
	// One call only rewrites up to MaxSegmentsPerRun sealed segments, and the
	// tiny MaxSegmentBytes here produces many more than that, so convergence
	// takes several sweeps.
	sweepToConvergence(t, inst, 20)
	requireRecordsEqual(t, expected, consume(t, c, name, 0, client.SubscribeOptions{}))

	// --- Window 2: crash between the data and index renames. ---
	// Compact once more (a no-op: everything is already deduplicated, so no
	// file changes) to get a clean "before" snapshot, then run a real
	// compaction pass again after a couple more publishes create fresh
	// duplicates, capturing index files before it and reverting whichever
	// ones it actually rewrote.
	more := []pubRecord{}
	for i := 28; i < 32; i++ {
		key := keys[i%len(keys)]
		offs, err := c.Publish(ctx, name, 0, client.Message{Key: key, Payload: []byte(fmt.Sprintf("v-%d", i))})
		require.NoError(t, err)
		more = append(more, pubRecord{offset: offs[0], key: key})
	}
	pubs = append(pubs, more...)
	survivors = expectedSurvivorOffsets(pubs)
	fullAfterMore := consume(t, c, name, 0, client.SubscribeOptions{})
	expected = filterRecords(fullAfterMore, survivors)

	// Capture the pre-compaction index files, compact for real, then stop the
	// instance cleanly (a graceful Close re-persists each segment's current
	// index, so the revert below must happen only after the process is fully
	// down — otherwise Close would immediately overwrite it with the correct
	// index again, defeating the simulation). Only then is reverting the
	// changed index files equivalent to a crash between the data and index
	// renames.
	preIndexes := snapshotFiles(t, dir, name, ".index")
	sweepToConvergence(t, inst, 20)
	inst.Stop(t)
	restoreChangedFiles(t, dir, name, preIndexes)

	inst = testutil.Start(t, startOpts())
	c = dial(t, inst)
	requireRecordsEqual(t, expected, consume(t, c, name, 0, client.SubscribeOptions{}))
}

// requireNoOrphanTemps asserts no ".tmp-*" file survives in the partition
// directory.
func requireNoOrphanTemps(t *testing.T, dir, name string) {
	t.Helper()
	pdir := partitionDir(dir, name, 0)
	entries, err := os.ReadDir(pdir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasPrefix(e.Name(), ".tmp-"), "orphan temp %q survived recovery", e.Name())
	}
}

// TestCompactionOffsetsContiguousDuringPublish covers docs/compaction-design.md
// sec 10's concurrency requirement: publishing while a compaction runs keeps
// offsets contiguous with no torn reads, and overlapping Compact calls
// serialize rather than corrupt anything (Log.Compact's own "compacting"
// flag).
func TestCompactionOffsetsContiguousDuringPublish(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{MaxSegmentBytes: 120})
	c := dial(t, inst)
	ctx := context.Background()
	const name = "compacted-live"
	_, err := c.CreateTopic(ctx, name, client.TopicConfig{Partitions: 1, Cleanup: "compact"})
	require.NoError(t, err)

	const publishers = 4
	const perPublisher = 50
	var mu sync.Mutex
	var allOffsets []int64

	var publishWG sync.WaitGroup
	publishWG.Add(publishers)
	for p := 0; p < publishers; p++ {
		go func(p int) {
			defer publishWG.Done()
			for i := 0; i < perPublisher; i++ {
				key := fmt.Sprintf("k%d", i%5) // deliberate duplicates within each publisher
				offs, err := c.Publish(ctx, name, 0, client.Message{
					Key:     key,
					Payload: []byte(fmt.Sprintf("p%d-%d", p, i)),
				})
				require.NoError(t, err)
				mu.Lock()
				allOffsets = append(allOffsets, offs...)
				mu.Unlock()
			}
		}(p)
	}

	// Compact concurrently with the publishers; overlapping calls must
	// serialize (or return a zero result) rather than tear a read or corrupt
	// the active segment, which Compact never touches.
	var stopCompacting atomic.Bool
	var compactWG sync.WaitGroup
	compactWG.Add(1)
	go func() {
		defer compactWG.Done()
		for !stopCompacting.Load() {
			inst.Broker().Sweep()
			time.Sleep(time.Millisecond)
		}
	}()

	publishWG.Wait()
	stopCompacting.Store(true)
	compactWG.Wait()

	// The address space Publish handed out must be exactly {0, ..., N-1}: no
	// offset skipped, duplicated, or torn by a compaction running alongside.
	total := publishers * perPublisher
	require.Len(t, allOffsets, total)
	seen := make(map[int64]bool, total)
	for _, o := range allOffsets {
		require.False(t, seen[o], "offset %d assigned twice", o)
		seen[o] = true
	}
	for o := int64(0); o < int64(total); o++ {
		require.True(t, seen[o], "offset %d never assigned", o)
	}

	// A full read must decode cleanly end to end (no torn batch) and never
	// return an offset outside the assigned range or duplicated.
	got := consume(t, c, name, 0, client.SubscribeOptions{})
	readSeen := make(map[int64]bool, len(got))
	for _, r := range got {
		require.False(t, readSeen[r.Offset], "read returned offset %d twice", r.Offset)
		readSeen[r.Offset] = true
		require.True(t, r.Offset >= 0 && r.Offset < int64(total), "read returned out-of-range offset %d", r.Offset)
	}
}
