// Package monitor serves the broker's operational HTTP surface: liveness and
// readiness probes, a JSON status dump, and (via the caller-supplied
// Gatherer) Prometheus metrics. It is separate from the gRPC transport so
// that a probe or scrape never competes with the data plane for a listener.
package monitor

import (
	"encoding/json"
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pulse-stream/pulse/internal/application/services"
	"github.com/pulse-stream/pulse/internal/domain/broker"
)

// statusResponse is the body of /healthz and /readyz.
type statusResponse struct {
	Status string `json:"status"`
}

// partitionVarz is one partition entry in the /varz topics list.
type partitionVarz struct {
	ID          int   `json:"id"`
	StartOffset int64 `json:"start_offset"`
	EndOffset   int64 `json:"end_offset"`
}

// topicVarz is one topic entry in the /varz topics list.
type topicVarz struct {
	Name       string          `json:"name"`
	Partitions []partitionVarz `json:"partitions"`
}

// varzResponse is the body of /varz.
type varzResponse struct {
	Version       string      `json:"version"`
	BrokerID      string      `json:"broker_id"`
	ClusterID     string      `json:"cluster_id"`
	State         string      `json:"state"`
	UptimeSeconds float64     `json:"uptime_seconds"`
	StartedAt     time.Time   `json:"started_at"`
	Topics        []topicVarz `json:"topics"`
	GoVersion     string      `json:"go_version"`
	NumGoroutine  int         `json:"num_goroutine"`
}

// New builds the monitor HTTP handler for broker b, reporting version and
// startedAt, and serving reg's collected metrics at /metrics.
func New(b *services.Broker, version string, startedAt time.Time, reg prometheus.Gatherer) http.Handler {
	mux := http.NewServeMux()

	// /healthz is liveness: the process is alive until the broker reaches
	// Stopped. /readyz is readiness: only a Running broker accepts work, so a
	// draining broker answers 503 here while still answering 200 on /healthz.
	status := func(w http.ResponseWriter, ok bool, state broker.State) {
		w.Header().Set("Content-Type", "application/json")
		if ok {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(statusResponse{Status: "ok"})
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(statusResponse{Status: state.String()})
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		state := b.State()
		status(w, state != broker.StateStopped, state)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		state := b.State()
		status(w, state == broker.StateRunning, state)
	})

	mux.HandleFunc("/varz", func(w http.ResponseWriter, _ *http.Request) {
		info := b.BrokerInfo()
		views := b.TopicsView()
		topics := make([]topicVarz, 0, len(views))
		for _, t := range views {
			partitions := make([]partitionVarz, 0, len(t.Partitions))
			for _, p := range t.Partitions {
				partitions = append(partitions, partitionVarz{ID: p.ID, StartOffset: p.StartOffset, EndOffset: p.EndOffset})
			}
			topics = append(topics, topicVarz{Name: t.Name, Partitions: partitions})
		}
		resp := varzResponse{
			Version:       version,
			BrokerID:      string(info.BrokerID),
			ClusterID:     string(info.ClusterID),
			State:         info.State.String(),
			UptimeSeconds: time.Since(startedAt).Seconds(),
			StartedAt:     startedAt,
			Topics:        topics,
			GoVersion:     runtime.Version(),
			NumGoroutine:  runtime.NumGoroutine(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	return mux
}
