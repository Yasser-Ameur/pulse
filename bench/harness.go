// Package main implements a single benchmark harness that drives Pulse and
// NATS JetStream through identical code paths.
//
// Fairness is the whole point of this file. The workload driver, the clock
// calls, the latency accumulation, and the percentile math live here and are
// shared by both backends. A backend contributes only three things: how to
// start its server, how to perform one durable publish, and how to replay a
// partition from the beginning. Everything that produces a number is common.
package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Publisher performs one synchronous durable publish. Publish must not return
// until the broker has acknowledged the record as durable, because that is the
// operation both systems are being measured on.
type Publisher interface {
	// Publish appends payload and blocks until the broker acknowledges it.
	Publish(ctx context.Context, payload []byte) error
	// Close releases the publisher's connection.
	Close() error
}

// Target is a broker under test. The harness owns process lifetime so that
// both systems are started, crashed, and restarted by the same code.
type Target interface {
	// Name is the label used in reports.
	Name() string
	// Start launches the broker process against dir and waits until it
	// accepts connections.
	Start(ctx context.Context, dir string) error
	// Setup creates the topic or stream the workload publishes to.
	Setup(ctx context.Context) error
	// NewPublisher opens one connection for one workload goroutine.
	NewPublisher() (Publisher, error)
	// Replay reads the partition from offset zero and returns how many
	// records it saw, stopping once it has seen want records.
	Replay(ctx context.Context, want int) (int, error)
	// Kill terminates the process without any cleanup, the way a crash does.
	Kill() error
}

// Workload is one benchmark run's parameters.
type Workload struct {
	// Messages is the number of measured publishes across all workers.
	Messages int
	// PayloadBytes is the size of each message payload.
	PayloadBytes int
	// Concurrency is the number of publisher goroutines, each with its own
	// connection and its own in-flight publish.
	Concurrency int
	// Warmup is the number of publishes discarded before measurement starts.
	Warmup int
}

// Percentiles summarizes a latency sample.
type Percentiles struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min"`
	Mean  time.Duration `json:"mean"`
	P50   time.Duration `json:"p50"`
	P90   time.Duration `json:"p90"`
	P99   time.Duration `json:"p99"`
	P999  time.Duration `json:"p999"`
	Max   time.Duration `json:"max"`
}

// PublishResult is one publish benchmark's outcome.
type PublishResult struct {
	Target       string        `json:"target"`
	Messages     int           `json:"messages"`
	PayloadBytes int           `json:"payload_bytes"`
	Concurrency  int           `json:"concurrency"`
	Elapsed      time.Duration `json:"elapsed"`
	MsgsPerSec   float64       `json:"msgs_per_sec"`
	MiBPerSec    float64       `json:"mib_per_sec"`
	Latency      Percentiles   `json:"latency"`
}

// summarize computes percentiles from an unsorted latency sample. It sorts in
// place.
func summarize(samples []time.Duration) Percentiles {
	if len(samples) == 0 {
		return Percentiles{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var total time.Duration
	for _, d := range samples {
		total += d
	}
	// quantile uses the nearest-rank method: the smallest value at or above
	// the requested fraction of the sample. No interpolation, so a reported
	// p99 is always a latency that actually occurred.
	quantile := func(q float64) time.Duration {
		rank := int(q * float64(len(samples)))
		if rank >= len(samples) {
			rank = len(samples) - 1
		}
		return samples[rank]
	}
	return Percentiles{
		Count: len(samples),
		Min:   samples[0],
		Mean:  total / time.Duration(len(samples)),
		P50:   quantile(0.50),
		P90:   quantile(0.90),
		P99:   quantile(0.99),
		P999:  quantile(0.999),
		Max:   samples[len(samples)-1],
	}
}

// RunPublish drives w against t and returns throughput and latency.
//
// Every worker opens its own connection, waits on a common start barrier, and
// then issues synchronous publishes back to back. Elapsed time is measured
// once around the whole measured phase, so throughput and the latency sample
// describe the same interval.
func RunPublish(ctx context.Context, t Target, w Workload) (PublishResult, error) {
	if w.Concurrency < 1 {
		return PublishResult{}, fmt.Errorf("concurrency must be at least 1")
	}
	payload := make([]byte, w.PayloadBytes)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	perWorker := w.Messages / w.Concurrency
	if perWorker < 1 {
		return PublishResult{}, fmt.Errorf("messages (%d) must be at least concurrency (%d)", w.Messages, w.Concurrency)
	}
	warmupPer := w.Warmup / w.Concurrency

	pubs := make([]Publisher, w.Concurrency)
	for i := range pubs {
		p, err := t.NewPublisher()
		if err != nil {
			return PublishResult{}, fmt.Errorf("open publisher %d: %w", i, err)
		}
		pubs[i] = p
	}
	defer func() {
		for _, p := range pubs {
			_ = p.Close()
		}
	}()

	// Warm up outside the measured window so that connection setup, file
	// preallocation, and page-cache effects do not land in the sample.
	for _, p := range pubs {
		for i := 0; i < warmupPer; i++ {
			if err := p.Publish(ctx, payload); err != nil {
				return PublishResult{}, fmt.Errorf("warmup publish: %w", err)
			}
		}
	}

	samples := make([][]time.Duration, w.Concurrency)
	errs := make([]error, w.Concurrency)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(w.Concurrency)
	done.Add(w.Concurrency)

	for i := range pubs {
		go func(i int, p Publisher) {
			defer done.Done()
			local := make([]time.Duration, 0, perWorker)
			ready.Done()
			<-start
			for n := 0; n < perWorker; n++ {
				t0 := time.Now()
				if err := p.Publish(ctx, payload); err != nil {
					errs[i] = err
					break
				}
				local = append(local, time.Since(t0))
			}
			samples[i] = local
		}(i, pubs[i])
	}

	ready.Wait()
	t0 := time.Now()
	close(start)
	done.Wait()
	elapsed := time.Since(t0)

	for i, err := range errs {
		if err != nil {
			return PublishResult{}, fmt.Errorf("worker %d: %w", i, err)
		}
	}

	merged := make([]time.Duration, 0, perWorker*w.Concurrency)
	for _, s := range samples {
		merged = append(merged, s...)
	}
	sent := len(merged)

	return PublishResult{
		Target:       t.Name(),
		Messages:     sent,
		PayloadBytes: w.PayloadBytes,
		Concurrency:  w.Concurrency,
		Elapsed:      elapsed,
		MsgsPerSec:   float64(sent) / elapsed.Seconds(),
		MiBPerSec:    float64(sent) * float64(w.PayloadBytes) / (1 << 20) / elapsed.Seconds(),
		Latency:      summarize(merged),
	}, nil
}

// RecoveryResult is one crash-recovery measurement.
type RecoveryResult struct {
	Target string `json:"target"`
	// Acked is how many publishes the broker acknowledged before the crash.
	Acked int `json:"acked"`
	// Recovered is how many of those records were readable after restart.
	Recovered int `json:"recovered"`
	// Restart is the time from launching the process to the first record
	// being served, which is what a client actually waits for.
	Restart time.Duration `json:"restart"`
	// Lost is Acked minus Recovered. Any value above zero is a durability
	// violation, because every counted publish was acknowledged as durable.
	Lost int `json:"lost"`
}

// RunRecovery publishes n durable records, kills the broker without cleanup,
// restarts it, and measures how long until every acknowledged record is
// readable again.
func RunRecovery(ctx context.Context, t Target, dir string, n, payloadBytes int) (RecoveryResult, error) {
	payload := make([]byte, payloadBytes)
	p, err := t.NewPublisher()
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("open publisher: %w", err)
	}
	acked := 0
	for i := 0; i < n; i++ {
		if err := p.Publish(ctx, payload); err != nil {
			_ = p.Close()
			return RecoveryResult{}, fmt.Errorf("publish %d: %w", i, err)
		}
		acked++
	}
	_ = p.Close()

	if err := t.Kill(); err != nil {
		return RecoveryResult{}, fmt.Errorf("kill: %w", err)
	}

	t0 := time.Now()
	if err := t.Start(ctx, dir); err != nil {
		return RecoveryResult{}, fmt.Errorf("restart: %w", err)
	}
	recovered, err := t.Replay(ctx, acked)
	restart := time.Since(t0)
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("replay: %w", err)
	}

	return RecoveryResult{
		Target:    t.Name(),
		Acked:     acked,
		Recovered: recovered,
		Restart:   restart,
		Lost:      acked - recovered,
	}, nil
}
