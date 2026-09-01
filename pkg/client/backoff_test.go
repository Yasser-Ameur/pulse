package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/testutil"
)

func TestNextBackoffDoublesAndCaps(t *testing.T) {
	want := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		2 * time.Second, // 3200ms would exceed the 2s cap
		2 * time.Second, // stays capped once it hits the ceiling
	}
	d := publishInitialBackoff
	for i, w := range want {
		if d != w {
			t.Fatalf("step %d: got %v, want %v", i, d, w)
		}
		d = nextBackoff(d)
	}
}

// TestPublishReturnsImmediatelyOnNonUnavailable pins that Publish only
// retries codes.Unavailable: a NotFound error (unknown topic) must come back
// on the first attempt, without paying any backoff delay.
func TestPublishReturnsImmediatelyOnNonUnavailable(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	c, err := Dial(inst.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	start := time.Now()
	_, err = c.Publish(context.Background(), "does-not-exist", 0, Message{Payload: []byte("x")})
	if err == nil {
		t.Fatal("want error publishing to a missing topic")
	}
	if elapsed := time.Since(start); elapsed > publishInitialBackoff {
		t.Fatalf("Publish took %v; a non-Unavailable error must not retry", elapsed)
	}
}

// TestPublishRetriesUntilDeadline pins that the caller's context, not an
// attempt count, is the retry budget: against a broker that is not listening
// the loop keeps retrying Unavailable until the deadline, then reports it.
func TestPublishRetriesUntilDeadline(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	addr := inst.Addr
	inst.Stop(t)

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	const budget = 400 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	_, err = c.Publish(ctx, "orders", 0, Message{Payload: []byte("x")})
	elapsed := time.Since(start)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Publish error = %v, want ErrUnavailable", err)
	}
	if elapsed < budget-100*time.Millisecond {
		t.Fatalf("Publish gave up after %v, want it to retry until the %v budget", elapsed, budget)
	}
}

// TestJitterNeverExceedsSchedule pins the full-jitter contract: the sleep
// applied for a given backoff step is a uniform random duration in [0, d],
// never more than the schedule value itself, and the schedule is still
// capped at publishMaxBackoff.
func TestJitterNeverExceedsSchedule(t *testing.T) {
	d := publishInitialBackoff
	for i := 0; i < 10; i++ {
		if d > publishMaxBackoff {
			t.Fatalf("step %d: schedule %v exceeds cap %v", i, d, publishMaxBackoff)
		}
		for j := 0; j < 50; j++ {
			if s := jitter(d); s < 0 || s > d {
				t.Fatalf("jitter(%v) = %v, want [0, %v]", d, s, d)
			}
		}
		d = nextBackoff(d)
	}
}
