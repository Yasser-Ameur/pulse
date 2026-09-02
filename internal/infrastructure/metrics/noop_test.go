package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TestNoopRecorderDiscardsEverything pins that every MetricsRecorder method
// on NoopRecorder is callable and returns nothing observable; it exists so a
// caller depending on the port never nil-checks it, and this test protects
// that contract from a future field or panic creeping in.
func TestNoopRecorderDiscardsEverything(t *testing.T) {
	r := NoopRecorder{}
	r.RecordPublish(1, 2)
	r.RecordConsume(1, 2)
	r.RecordPublishLatency(time.Millisecond)
	r.RecordConsumeLatency(time.Millisecond)
	r.RecordBytesWritten(1)
	r.RecordBytesRead(1)
}

func TestRegisterBrokerInfo(t *testing.T) {
	reg := prometheus.NewRegistry()
	RegisterBrokerInfo(reg, "1.2.3")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	var found bool
	for _, mf := range families {
		if mf.GetName() != "pulse_broker_info" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if m.GetGauge().GetValue() != 1 {
				t.Fatalf("pulse_broker_info value = %v, want 1", m.GetGauge().GetValue())
			}
			for _, l := range m.GetLabel() {
				if l.GetName() == "version" {
					found = true
					if l.GetValue() != "1.2.3" {
						t.Fatalf("version label = %q, want %q", l.GetValue(), "1.2.3")
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("pulse_broker_info metric with a version label was not registered")
	}
}
