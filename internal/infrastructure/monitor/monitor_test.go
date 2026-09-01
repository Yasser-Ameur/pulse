package monitor_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pulse-stream/pulse/internal/infrastructure/monitor"
	"github.com/pulse-stream/pulse/internal/testutil"
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
	h := monitor.New(inst.Broker(), "test", time.Now(), prometheus.NewRegistry())

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
