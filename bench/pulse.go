package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/internal/infrastructure/grpc/client"
)

// pulseTarget runs pulse-server with sync-mode: every-write, which fsyncs the
// segment on every append. That is the setting matched against JetStream's
// sync_interval: always.
type pulseTarget struct {
	bin   string
	addr  string
	topic topic.Name
	sync  bool
	cmd   *exec.Cmd
	admin *client.Client
}

func newPulseTarget(bin string, sync bool) (*pulseTarget, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	return &pulseTarget{
		bin:   bin,
		addr:  fmt.Sprintf("127.0.0.1:%d", port),
		topic: topic.Name("bench"),
		sync:  sync,
	}, nil
}

func (p *pulseTarget) Name() string { return "pulse" }

func (p *pulseTarget) Start(ctx context.Context, dir string) error {
	// every-write fsyncs every append. The relaxed arm matches JetStream's
	// relaxed arm so the fsync cost can be isolated on both sides.
	mode := "every-write"
	if !p.sync {
		mode = "interval"
	}
	cfg := fmt.Sprintf(""+
		"listen-addr: %q\n"+
		"data-dir: %q\n"+
		"log-level: error\n"+
		"storage:\n"+
		"  sync-mode: %s\n",
		p.addr, filepath.ToSlash(dir), mode)
	path := filepath.Join(dir, "pulse.yaml")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		return err
	}

	cmd := exec.Command(p.bin, "--config", path)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start pulse-server: %w", err)
	}
	p.cmd = cmd

	// Dial inside the probe rather than once before it. A single long-lived
	// channel dialled before the server is listening enters gRPC's reconnect
	// backoff, whose default base delay is one second, so the probe cannot
	// succeed sooner than that no matter how fast the broker came up. The
	// JetStream probe opens a fresh connection per attempt, so this keeps the
	// two readiness measurements symmetric.
	return waitReady(ctx, func() error {
		c, err := client.Dial(p.addr)
		if err != nil {
			return err
		}
		if _, err := c.BrokerInfo(ctx); err != nil {
			_ = c.Close()
			return err
		}
		p.admin = c
		return nil
	})
}

func (p *pulseTarget) Setup(ctx context.Context) error {
	_, err := p.admin.CreateTopic(ctx, p.topic.String(), topic.DefaultConfig(), 1)
	return err
}

func (p *pulseTarget) NewPublisher() (Publisher, error) {
	c, err := client.Dial(p.addr)
	if err != nil {
		return nil, err
	}
	return &pulsePublisher{c: c, topic: p.topic}, nil
}

func (p *pulseTarget) Replay(ctx context.Context, want int) (int, error) {
	c, err := client.Dial(p.addr)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	start := offset.Offset(0)
	sub := consumer.Subscription{
		Topic:     p.topic,
		Partition: 0,
		Start:     &start,
		Follow:    false,
	}
	seen := 0
	stop := errStop
	err = c.Subscribe(ctx, sub, func(message.Record) error {
		seen++
		if seen >= want {
			return stop
		}
		return nil
	})
	if err != nil && err != stop {
		return seen, err
	}
	return seen, nil
}

func (p *pulseTarget) Kill() error {
	if p.admin != nil {
		_ = p.admin.Close()
		p.admin = nil
	}
	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	_ = p.cmd.Wait()
	p.cmd = nil
	// Give the OS a moment to release the listening socket so the restarted
	// process can bind the same port.
	time.Sleep(200 * time.Millisecond)
	return nil
}

type pulsePublisher struct {
	c     *client.Client
	topic topic.Name
	buf   [1]message.Message
}

func (p *pulsePublisher) Publish(ctx context.Context, payload []byte) error {
	p.buf[0] = message.Message{Payload: payload}
	_, err := p.c.Publish(ctx, p.topic, 0, p.buf[:])
	return err
}

func (p *pulsePublisher) Close() error { return p.c.Close() }

// errStop unwinds a Subscribe callback once enough records have been seen.
var errStop = fmt.Errorf("bench: replay complete")

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	return port, l.Close()
}

// waitReady polls probe until it succeeds or ctx expires.
func waitReady(ctx context.Context, probe func() error) error {
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if last = probe(); last == nil {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("broker did not become ready: %w", last)
}
