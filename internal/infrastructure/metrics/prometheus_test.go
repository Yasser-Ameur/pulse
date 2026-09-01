package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPrometheusRecorder(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPrometheusRecorder(reg)

	r.RecordPublish(3, 100)
	r.RecordConsume(2, 40)
	r.RecordPublishLatency(10 * time.Millisecond)
	r.RecordConsumeLatency(5 * time.Millisecond)
	r.RecordBytesWritten(1024)
	r.RecordBytesRead(512)
	r.SetUp(true)

	if got := testutil.ToFloat64(r.publishRecords); got != 3 {
		t.Errorf("pulse_publish_records_total = %v, want 3", got)
	}
	if got := testutil.ToFloat64(r.publishBytes); got != 100 {
		t.Errorf("pulse_publish_bytes_total = %v, want 100", got)
	}
	if got := testutil.ToFloat64(r.consumeRecords); got != 2 {
		t.Errorf("pulse_consume_records_total = %v, want 2", got)
	}
	if got := testutil.ToFloat64(r.consumeBytes); got != 40 {
		t.Errorf("pulse_consume_bytes_total = %v, want 40", got)
	}
	if got := testutil.ToFloat64(r.bytesWritten); got != 1024 {
		t.Errorf("pulse_storage_bytes_written_total = %v, want 1024", got)
	}
	if got := testutil.ToFloat64(r.bytesRead); got != 512 {
		t.Errorf("pulse_storage_bytes_read_total = %v, want 512", got)
	}
	if got := testutil.ToFloat64(r.up); got != 1 {
		t.Errorf("pulse_up = %v, want 1", got)
	}

	r.SetUp(false)
	if got := testutil.ToFloat64(r.up); got != 0 {
		t.Errorf("pulse_up = %v, want 0 after SetUp(false)", got)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	names := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"pulse_publish_records_total",
		"pulse_publish_bytes_total",
		"pulse_consume_records_total",
		"pulse_consume_bytes_total",
		"pulse_publish_latency_seconds",
		"pulse_consume_latency_seconds",
		"pulse_storage_bytes_written_total",
		"pulse_storage_bytes_read_total",
		"pulse_up",
	} {
		if !names[want] {
			t.Errorf("registry missing metric %q", want)
		}
	}
}
