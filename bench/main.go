package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	var (
		targets  = flag.String("target", "pulse,jetstream", "comma-separated targets: pulse, jetstream")
		mode     = flag.String("mode", "publish", "benchmark mode: publish or recovery")
		messages = flag.Int("n", 20000, "measured publishes per run")
		size     = flag.Int("size", 256, "payload bytes per message")
		concs    = flag.String("conc", "1,8,32", "comma-separated publisher concurrency levels")
		warmup   = flag.Int("warmup", 1000, "publishes discarded before measurement")
		pulseBin = flag.String("pulse-bin", filepath.Join("..", "bin", "pulse-server.exe"), "path to pulse-server binary")
		natsBin  = flag.String("nats-bin", "nats-server", "path to nats-server binary")
		root     = flag.String("dir", "tmp", "scratch directory for broker data")
		out      = flag.String("out", "", "write results as JSON to this path")
		sync     = flag.Bool("sync", true, "fsync every write on both brokers; false runs the relaxed control arm")
	)
	flag.Parse()

	if err := run(*targets, *mode, *messages, *size, *concs, *warmup, *pulseBin, *natsBin, *root, *out, *sync); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run(targets, mode string, messages, size int, concs string, warmup int, pulseBin, natsBin, root, out string, sync bool) error {
	ctx := context.Background()

	natsPath, err := exec.LookPath(natsBin)
	if err != nil {
		return fmt.Errorf("nats-server not found (%s): %w", natsBin, err)
	}
	if _, err := os.Stat(pulseBin); err != nil {
		return fmt.Errorf("pulse-server binary not found at %s: build it first with: go build -o %s ./cmd/pulse-server", pulseBin, pulseBin)
	}

	levels, err := parseInts(concs)
	if err != nil {
		return fmt.Errorf("parse -conc: %w", err)
	}

	newTarget := func(name string) (Target, error) {
		switch name {
		case "pulse":
			return newPulseTarget(pulseBin, sync)
		case "jetstream":
			return newJetStreamTarget(natsPath, sync)
		default:
			return nil, fmt.Errorf("unknown target %q", name)
		}
	}

	switch mode {
	case "publish":
		var results []PublishResult
		for _, name := range strings.Split(targets, ",") {
			for _, c := range levels {
				r, err := publishRun(ctx, newTarget, strings.TrimSpace(name), root, Workload{
					Messages:     messages,
					PayloadBytes: size,
					Concurrency:  c,
					Warmup:       warmup,
				})
				if err != nil {
					return err
				}
				results = append(results, r)
				printPublish(r)
			}
		}
		return writeJSON(out, results)

	case "recovery":
		var results []RecoveryResult
		for _, name := range strings.Split(targets, ",") {
			r, err := recoveryRun(ctx, newTarget, strings.TrimSpace(name), root, messages, size)
			if err != nil {
				return err
			}
			results = append(results, r)
			printRecovery(r)
		}
		return writeJSON(out, results)

	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
}

// publishRun gives every run a fresh data directory so that no run inherits
// another run's segments, page cache warmth, or stream state.
func publishRun(ctx context.Context, newTarget func(string) (Target, error), name, root string, w Workload) (PublishResult, error) {
	t, err := newTarget(name)
	if err != nil {
		return PublishResult{}, err
	}
	dir := runDir(root, name, w.Concurrency)
	defer func() { _ = t.Kill() }()

	if err := t.Start(ctx, dir); err != nil {
		return PublishResult{}, err
	}
	if err := t.Setup(ctx); err != nil {
		return PublishResult{}, fmt.Errorf("%s setup: %w", name, err)
	}
	return RunPublish(ctx, t, w)
}

func recoveryRun(ctx context.Context, newTarget func(string) (Target, error), name, root string, n, size int) (RecoveryResult, error) {
	t, err := newTarget(name)
	if err != nil {
		return RecoveryResult{}, err
	}
	dir := runDir(root, name, 1)
	defer func() { _ = t.Kill() }()

	if err := t.Start(ctx, dir); err != nil {
		return RecoveryResult{}, err
	}
	if err := t.Setup(ctx); err != nil {
		return RecoveryResult{}, fmt.Errorf("%s setup: %w", name, err)
	}
	return RunRecovery(ctx, t, dir, n, size)
}

func runDir(root, name string, conc int) string {
	return filepath.Join(root, fmt.Sprintf("%s-c%d-%d", name, conc, time.Now().UnixNano()))
}

func printPublish(r PublishResult) {
	fmt.Printf("%-10s conc=%-3d n=%-7d size=%-5d  %9.0f msg/s  %6.2f MiB/s  p50=%-9s p99=%-9s p999=%-9s max=%s\n",
		r.Target, r.Concurrency, r.Messages, r.PayloadBytes,
		r.MsgsPerSec, r.MiBPerSec,
		round(r.Latency.P50), round(r.Latency.P99), round(r.Latency.P999), round(r.Latency.Max))
}

func printRecovery(r RecoveryResult) {
	fmt.Printf("%-10s acked=%-7d recovered=%-7d lost=%-4d restart=%s\n",
		r.Target, r.Acked, r.Recovered, r.Lost, round(r.Restart))
}

// round trims latencies to microsecond precision so the columns stay readable.
func round(d time.Duration) time.Duration { return d.Round(time.Microsecond) }

func writeJSON(path string, v any) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func parseInts(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}
