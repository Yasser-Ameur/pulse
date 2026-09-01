package client

import (
	"context"
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
