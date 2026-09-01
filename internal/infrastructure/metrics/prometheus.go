package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pulse-stream/pulse/internal/application/ports"
)

// PrometheusRecorder implements ports.MetricsRecorder over a caller-supplied
// registry, so the broker's data-plane counters and histograms are served
// alongside the Go and process collectors at the monitor listener's
// /metrics endpoint.
type PrometheusRecorder struct {
	publishRecords prometheus.Counter
	publishBytes   prometheus.Counter
	consumeRecords prometheus.Counter
	consumeBytes   prometheus.Counter
	publishLatency prometheus.Histogram
	consumeLatency prometheus.Histogram
	bytesWritten   prometheus.Counter
	bytesRead      prometheus.Counter
	up             prometheus.Gauge
}

// NewPrometheusRecorder registers the broker's metrics on reg and returns a
// MetricsRecorder that records into them.
func NewPrometheusRecorder(reg prometheus.Registerer) *PrometheusRecorder {
	r := &PrometheusRecorder{
		publishRecords: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_publish_records_total",
			Help: "Total records published.",
		}),
		publishBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_publish_bytes_total",
			Help: "Total payload bytes published.",
		}),
		consumeRecords: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_consume_records_total",
			Help: "Total records delivered to consumers.",
		}),
		consumeBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_consume_bytes_total",
			Help: "Total payload bytes delivered to consumers.",
		}),
		publishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pulse_publish_latency_seconds",
			Help: "Publish handler latency.",
		}),
		consumeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pulse_consume_latency_seconds",
			Help: "Subscribe read loop latency.",
		}),
		bytesWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_storage_bytes_written_total",
			Help: "Total bytes durably written to the data plane.",
		}),
		bytesRead: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulse_storage_bytes_read_total",
			Help: "Total bytes read from the data plane.",
		}),
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulse_up",
			Help: "1 if the broker is running, 0 otherwise.",
		}),
	}
	reg.MustRegister(
		r.publishRecords, r.publishBytes,
		r.consumeRecords, r.consumeBytes,
		r.publishLatency, r.consumeLatency,
		r.bytesWritten, r.bytesRead,
		r.up,
	)
	return r
}

// RegisterBrokerInfo registers the pulse_broker_info gauge, labeled with
// version, set to 1.
func RegisterBrokerInfo(reg prometheus.Registerer, version string) {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "pulse_broker_info",
		Help:        "Always 1; labeled with the running broker version.",
		ConstLabels: prometheus.Labels{"version": version},
	})
	g.Set(1)
	reg.MustRegister(g)
}

// SetUp sets the pulse_up gauge.
func (r *PrometheusRecorder) SetUp(up bool) {
	if up {
		r.up.Set(1)
		return
	}
	r.up.Set(0)
}

// RecordPublish implements ports.MetricsRecorder.
func (r *PrometheusRecorder) RecordPublish(records, bytes int) {
	r.publishRecords.Add(float64(records))
	r.publishBytes.Add(float64(bytes))
}

// RecordConsume implements ports.MetricsRecorder.
func (r *PrometheusRecorder) RecordConsume(records, bytes int) {
	r.consumeRecords.Add(float64(records))
	r.consumeBytes.Add(float64(bytes))
}

// RecordPublishLatency implements ports.MetricsRecorder.
func (r *PrometheusRecorder) RecordPublishLatency(d time.Duration) {
	r.publishLatency.Observe(d.Seconds())
}

// RecordConsumeLatency implements ports.MetricsRecorder.
func (r *PrometheusRecorder) RecordConsumeLatency(d time.Duration) {
	r.consumeLatency.Observe(d.Seconds())
}

// RecordBytesWritten implements ports.MetricsRecorder.
func (r *PrometheusRecorder) RecordBytesWritten(n int64) {
	r.bytesWritten.Add(float64(n))
}

// RecordBytesRead implements ports.MetricsRecorder.
func (r *PrometheusRecorder) RecordBytesRead(n int64) {
	r.bytesRead.Add(float64(n))
}

var _ ports.MetricsRecorder = (*PrometheusRecorder)(nil)
