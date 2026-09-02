package metadata

import (
	"context"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

// dirDeleteOutcome is what reopening a mutilated store directory did.
type dirDeleteOutcome struct {
	panicked bool
	panicVal any
	stack    []byte
	err      error
	topicOK  bool
}

// dirDeleteReopen reopens dir, converting a panic into a value rather than
// letting it kill the test binary.
func dirDeleteReopen(t *testing.T, dir string, topicName topic.Name) (out dirDeleteOutcome) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			out.panicked = true
			out.panicVal = r
			out.stack = debug.Stack()
		}
	}()
	s, err := OpenPebble(dir)
	out.err = err
	if err != nil {
		return out
	}
	_, ok, gerr := s.GetTopic(context.Background(), topicName)
	out.topicOK = ok
	if gerr != nil {
		out.err = gerr
	}
	// Best effort: a store that opened is ours to close.
	_ = s.Close()
	return out
}

// dirDeleteFixture builds a store directory holding one topic, closes it
// cleanly, then deletes every file for which keep returns false. It returns the
// directory and the names kept and dropped.
//
// t.TempDir is deliberately NOT used. A reopen that panics leaves Pebble
// half-initialized and still holding Windows file handles, so the automatic
// RemoveAll fails and marks the test failed for a reason unrelated to what it
// measures. Cleanup here is best effort.
func dirDeleteFixture(t *testing.T, keep func(name string) bool) (dir string, kept, dropped []string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "pulse-dirdelete-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	s, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble() error = %v", err)
	}
	if err := s.CreateTopic(context.Background(), testTopic("dirdelete", 1)); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	for _, e := range ents {
		if keep(e.Name()) {
			kept = append(kept, e.Name())
			continue
		}
		dropped = append(dropped, e.Name())
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			t.Fatalf("remove %s: %v", e.Name(), err)
		}
	}
	if len(dropped) == 0 {
		t.Fatalf("fixture dropped nothing; kept=%v", kept)
	}
	return dir, kept, dropped
}

func dirDeleteIsManifest(name string) bool {
	return name == "CURRENT" || strings.HasPrefix(name, "MANIFEST-") || strings.HasPrefix(name, "marker.")
}

func dirDeleteIsWAL(name string) bool { return strings.HasSuffix(name, ".log") }

// TestPebbleDirDeleteWALWithoutManifest reopens a store directory whose write
// ahead log survived but whose MANIFEST and CURRENT marker were deleted.
//
// This state is NOT reachable by killing the process. Pebble writes the
// MANIFEST and the CURRENT marker in versionSet.create before any .log file
// exists, and on every reopen it commits the new version through logAndApply
// before creating the next .log. So no crash point yields a WAL with no
// manifest; only an external actor deleting files can, e.g. an rm -rf racing a
// live process, which on Windows removes the files whose handles are closed and
// leaves the ones still held.
//
// What the reopen must not do is silently succeed and serve an empty store.
// Pebble v1.1.5 does not: it dereferences a nil pointer. Recovery from the WAL
// runs newFlush against the freshly created version, whose L0Sublevels is nil
// because versionSet.create never calls InitL0Sublevels. newFlush guards that
// nil for c.l0Limits but setupInuseKeyRanges, reached on the same path, does
// not, and calls v.L0Sublevels.InUseKeyRanges. That missing guard is upstream,
// not in Pulse; Pulse's only exposure is that OpenPebble propagates the panic.
//
// The assertion is the safety property, not the specific failure mode, so an
// upstream fix that returns an error instead keeps this test green.
func TestPebbleDirDeleteWALWithoutManifest(t *testing.T) {
	dir, kept, dropped := dirDeleteFixture(t, func(n string) bool { return !dirDeleteIsManifest(n) })
	t.Logf("kept=%v dropped=%v", kept, dropped)

	var sawWAL bool
	for _, n := range kept {
		if dirDeleteIsWAL(n) {
			sawWAL = true
		}
	}
	if !sawWAL {
		t.Fatalf("fixture kept no write ahead log; kept=%v", kept)
	}

	out := dirDeleteReopen(t, dir, testTopic("dirdelete", 1).Name)
	switch {
	case out.panicked:
		t.Logf("reopen panicked (pebble v1.1.5 behaviour): %v", out.panicVal)
		if !strings.Contains(string(out.stack), "setupInuseKeyRanges") {
			t.Errorf("panic did not come from the documented path; stack:\n%s", out.stack)
		}
	case out.err != nil:
		t.Logf("reopen returned an error: %v", out.err)
	case !out.topicOK:
		t.Fatalf("reopen succeeded with err = nil and silently served an empty store: "+
			"the topic written before the manifest was deleted is gone, with nothing reported. kept=%v", kept)
	}
}

// TestPebbleDirDeleteManifestWithoutWAL reopens a store directory whose
// MANIFEST and CURRENT marker survived but whose write ahead log was deleted.
//
// Unlike the case above this one does not fail loudly. Pebble opens, finds no
// log to replay, and returns a healthy store; the unflushed topic is simply
// absent. The point of the test is that the loss is reported nowhere: err is
// nil at open and nil at read, and the caller cannot tell an emptied store from
// a store that never held the topic.
func TestPebbleDirDeleteManifestWithoutWAL(t *testing.T) {
	dir, kept, dropped := dirDeleteFixture(t, func(n string) bool { return !dirDeleteIsWAL(n) })
	t.Logf("kept=%v dropped=%v", kept, dropped)

	out := dirDeleteReopen(t, dir, testTopic("dirdelete", 1).Name)
	if out.panicked {
		t.Fatalf("reopen panicked: %v\n%s", out.panicVal, out.stack)
	}
	if out.err != nil {
		t.Fatalf("reopen error = %v, want nil", out.err)
	}
	if out.topicOK {
		t.Fatalf("topic survived deletion of the write ahead log that held it")
	}
	t.Log("data loss is silent: OpenPebble err = nil, GetTopic ok = false, err = nil")
}
