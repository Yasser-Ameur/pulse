// Crash-at-any-point recovery stress. Each round publishes records, records
// their exact (offset, payload, timestamp) into an in-memory reference model,
// then simulates a crash at a different point and restarts the broker over the
// same data directory. After every restart the broker must return exactly a
// prefix of the reference model with identical payloads and timestamps, and
// offsets must remain contiguous from zero.
package integration

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/index"
	"github.com/pulse-stream/pulse/internal/infrastructure/storage/engine/snapshot"
	"github.com/pulse-stream/pulse/internal/testutil"
	"github.com/pulse-stream/pulse/pkg/client"
	"github.com/stretchr/testify/require"
)

// TestCrashAtAnyPointRecovery varies the simulated crash point across rounds:
//
//	0 — graceful stop: the close-time snapshot makes recovery take the exact
//	    snapshot fast path.
//	1 — the checkpoint is rewound to an earlier batch boundary and the active
//	    segment tail is truncated: recovery trusts the durable prefix up to the
//	    checkpoint and scans only the delta tail, dropping torn bytes.
//	2 — no snapshot (deleted) and a torn active tail: recovery full-scans and
//	    truncates at the last valid batch.
//
// The final round is always a graceful restart so the reference model is
// recovered in full.
func TestCrashAtAnyPointRecovery(t *testing.T) {
	dir := t.TempDir()
	const name = "crash"
	ctx := context.Background()

	// A small index interval yields several batch boundaries per segment, so
	// the rewind simulation can pick a genuine mid-log checkpoint.
	startOpts := func() testutil.Options { return testutil.Options{Dir: dir, IndexInterval: 64} }

	// reference is the broker's authoritative log after this round's publish.
	// recovered is what the previous restart recovered (a prefix of reference).
	var reference, recovered []client.Record
	topicCreated := false

	inst := testutil.Start(t, startOpts())
	c := dial(t, inst)

	const rounds = 7
	for round := 0; round < rounds; round++ {
		if !topicCreated {
			_, err := c.CreateTopic(ctx, name, client.TopicConfig{Partitions: 1})
			require.NoError(t, err)
			topicCreated = true
		}

		msgs := make([]client.Message, 0, 6)
		for i := 0; i < 6; i++ {
			msgs = append(msgs, client.Message{
				Key:         "k",
				Payload:     []byte(fmt.Sprintf("r%d-%d", round, i)),
				ContentType: "text/plain",
			})
		}
		offs, err := c.Publish(ctx, name, 0, msgs...)
		require.NoError(t, err)
		require.Len(t, offs, len(msgs))
		require.Equal(t, int64(len(recovered)), offs[0], "round %d first offset", round)

		// Capture the broker's authoritative log into the reference model.
		reference = consume(t, c, name, 0, client.SubscribeOptions{})
		require.Len(t, reference, len(recovered)+len(msgs), "round %d reference length", round)

		// Graceful stop first: the close writes a valid snapshot, which the
		// simulation below then rewinds or discards to model a crash at an
		// arbitrary earlier point.
		inst.Stop(t)
		switch round % 3 {
		case 0:
			// Nothing to simulate; the snapshot is used as-is.
		case 1:
			rewindCheckpointAndTornTail(t, dir, name)
		case 2:
			removeSnapshots(t, dir, name)
			tornTail(t, dir, name)
		}

		// Restart and verify recovery against the reference model.
		inst = testutil.Start(t, startOpts())
		c = dial(t, inst)
		recovered = consume(t, c, name, 0, client.SubscribeOptions{})
		require.True(t, len(recovered) <= len(reference),
			"round %d recovered %d records, want <= %d", round, len(recovered), len(reference))
		requirePrefix(t, reference, recovered)
		requireContiguous(t, recovered)

		// The final round must recover every published record.
		if round == rounds-1 {
			require.Len(t, recovered, len(reference), "final round must recover the full reference model")
		}
	}
}

// requirePrefix asserts got is a prefix of want with identical payloads and
// timestamps.
func requirePrefix(t *testing.T, want, got []client.Record) {
	t.Helper()
	for i := range got {
		w, g := want[i], got[i]
		require.Equal(t, w.Offset, g.Offset, "record %d offset", i)
		require.Equal(t, string(w.Message.Payload), string(g.Message.Payload), "record %d payload", i)
		require.True(t, w.Timestamp.Equal(g.Timestamp), "record %d timestamp %v vs %v", i, w.Timestamp, g.Timestamp)
		require.Equal(t, w.Message.Key, g.Message.Key, "record %d key", i)
		require.Equal(t, w.Message.ContentType, g.Message.ContentType, "record %d content type", i)
	}
}

// requireContiguous asserts the recovered offsets start at zero with no gaps.
func requireContiguous(t *testing.T, got []client.Record) {
	t.Helper()
	for i, r := range got {
		require.Equal(t, int64(i), r.Offset, "offset %d", r.Offset)
	}
}

func partitionDir(dir string, name string, id int32) string {
	return filepath.Join(dir, "topics", name, strconv.Itoa(int(id)))
}

// segmentFiles returns the partition's .log files sorted by segment name.
func segmentFiles(t *testing.T, dir string, name string) []string {
	t.Helper()
	entries, err := os.ReadDir(partitionDir(dir, name, 0))
	require.NoError(t, err)
	var files []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".log" {
			continue
		}
		files = append(files, filepath.Join(partitionDir(dir, name, 0), e.Name()))
	}
	sort.Strings(files)
	return files
}

func activeSegmentFile(t *testing.T, dir string, name string) string {
	t.Helper()
	files := segmentFiles(t, dir, name)
	require.NotEmpty(t, files, "no segment files to truncate")
	return files[len(files)-1]
}

// truncateFile resizes path to size bytes, or removes it when size is zero.
func truncateFile(t *testing.T, path string, size int64) {
	t.Helper()
	if size <= 0 {
		require.NoError(t, os.Remove(path))
		return
	}
	require.NoError(t, os.Truncate(path, size))
}

// removeSnapshots deletes every partition checkpoint so startup must fall back
// to a full scan.
func removeSnapshots(t *testing.T, dir string, name string) {
	t.Helper()
	require.NoError(t, os.Remove(snapshot.Path(partitionDir(dir, name, 0))))
}

// tornTail truncates the active segment to a random size, simulating a crash
// mid-append.
func tornTail(t *testing.T, dir string, name string) {
	t.Helper()
	path := activeSegmentFile(t, dir, name)
	fi, err := os.Stat(path)
	require.NoError(t, err)
	if fi.Size() < 2 {
		return
	}
	truncateFile(t, path, 1+rand.Int63n(fi.Size()-1))
}

// rewindCheckpointAndTornTail rewinds the snapshot's ActiveSize to an earlier
// batch boundary (real checkpoints always sit on a batch boundary) so recovery
// must trust the durable prefix and scan only the delta tail, which is then
// truncated at a random point — possibly mid-batch.
func rewindCheckpointAndTornTail(t *testing.T, dir string, name string) {
	t.Helper()
	pdir := partitionDir(dir, name, 0)
	path := activeSegmentFile(t, dir, name)
	fi, err := os.Stat(path)
	require.NoError(t, err)

	st, present, err := snapshot.Load(pdir)
	require.NoError(t, err)
	require.True(t, present, "expected a snapshot after graceful stop")

	if fi.Size() < 2 {
		return
	}
	rewound := st
	if pos, ok := batchBoundaryBefore(t, path, fi.Size()); ok && pos > 0 {
		rewound.ActiveSize = pos
	} else {
		// No batch-aligned rewind point available; fall back to a plain torn
		// tail (still a valid prefix test).
		tornTail(t, dir, name)
		return
	}
	require.NoError(t, snapshot.Write(pdir, rewound))

	// Truncate the tail to a random point at or after the rewound checkpoint.
	low := rewound.ActiveSize
	if fi.Size() <= low+1 {
		low = 0
	}
	truncateFile(t, path, low+rand.Int63n(fi.Size()-low))
}

// batchBoundaryBefore decodes the active segment's sparse index and returns a
// batch start position strictly before limit.
func batchBoundaryBefore(t *testing.T, logPath string, limit int64) (int64, bool) {
	t.Helper()
	data, err := os.ReadFile(strings.TrimSuffix(logPath, ".log") + ".index")
	if err != nil {
		return 0, false
	}
	ix, err := index.Decode(data, offset.Offset(0))
	if err != nil {
		return 0, false
	}
	var candidates []int64
	for _, e := range ix.Entries() {
		p := int64(e.RelativePosition)
		if p > 0 && p < limit {
			candidates = append(candidates, p)
		}
	}
	if len(candidates) == 0 {
		return 0, false
	}
	return candidates[rand.Intn(len(candidates))], true
}
