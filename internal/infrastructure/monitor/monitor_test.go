package monitor_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/pulse-stream/pulse/internal/infrastructure/monitor"
	"github.com/pulse-stream/pulse/internal/testutil"
	"github.com/pulse-stream/pulse/pkg/client"
)

func get(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// TestLivenessOutlivesReadiness pins the probe semantics: a draining broker
// is still alive but no longer ready, and only a stopped broker is not alive.
func TestLivenessOutlivesReadiness(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	h := monitor.New(inst.Broker(), "test", time.Now(), prometheus.NewRegistry(), func() int64 { return 0 })

	if got := get(t, h, "/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz while running = %d, want 200", got)
	}
	if got := get(t, h, "/readyz"); got != http.StatusOK {
		t.Fatalf("/readyz while running = %d, want 200", got)
	}

	inst.Broker().Drain()
	if got := get(t, h, "/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz while draining = %d, want 200", got)
	}
	if got := get(t, h, "/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while draining = %d, want 503", got)
	}

	inst.Stop(t)
	if got := get(t, h, "/healthz"); got != http.StatusServiceUnavailable {
		t.Fatalf("/healthz after stop = %d, want 503", got)
	}
	if got := get(t, h, "/varz"); got != http.StatusOK {
		t.Fatalf("/varz after stop = %d, want 200", got)
	}
}

// varzBody is the subset of /varz fields TestVarzReportsStats checks.
type varzBody struct {
	Subscriptions    int64 `json:"subscriptions"`
	PublishedRecords int64 `json:"published_records"`
	PublishedBytes   int64 `json:"published_bytes"`
	DeliveredRecords int64 `json:"delivered_records"`
	DeliveredBytes   int64 `json:"delivered_bytes"`
}

// TestVarzReportsStats pins that /varz surfaces the broker's Stats counters
// after one publish and one live (follow) subscribe.
func TestVarzReportsStats(t *testing.T) {
	inst := testutil.Start(t, testutil.Options{})
	h := monitor.New(inst.Broker(), "test", time.Now(), prometheus.NewRegistry(), func() int64 { return 0 })

	c, err := client.Dial(inst.Addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	_, err = c.CreateTopic(ctx, "orders", client.TopicConfig{Partitions: 1})
	require.NoError(t, err)

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = c.Subscribe(subCtx, "orders", 0, client.SubscribeOptions{Follow: true},
			func(client.Record) error { return nil })
	}()

	// Poll until the subscription is registered before publishing and
	// checking /varz, rather than sleeping for an arbitrary duration.
	require.Eventually(t, func() bool {
		return inst.Broker().Stats().Subscriptions == 1
	}, 2*time.Second, 5*time.Millisecond)

	_, err = c.Publish(ctx, "orders", 0, client.Message{Payload: []byte("hello")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return inst.Broker().Stats().DeliveredRecords > 0
	}, 2*time.Second, 5*time.Millisecond)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/varz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var body varzBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, int64(1), body.Subscriptions)
	require.Equal(t, int64(1), body.PublishedRecords)
	require.Greater(t, body.PublishedBytes, int64(0))
	require.Equal(t, int64(1), body.DeliveredRecords)
	require.Greater(t, body.DeliveredBytes, int64(0))
}
