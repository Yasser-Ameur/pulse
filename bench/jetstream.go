package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
)

// natsSubject is the single subject the JetStream stream is bound to. One
// subject on a one-replica file-backed stream is the closest JetStream shape
// to a single Pulse partition.
const natsSubject = "bench"

// jetstreamTarget runs nats-server with JetStream file storage and
// sync_interval: always, which fsyncs on every write. That is the setting
// matched against Pulse's sync-mode: every-write.
type jetstreamTarget struct {
	bin  string
	addr string
	url  string
	sync bool
	cmd  *exec.Cmd
}

func newJetStreamTarget(bin string, sync bool) (*jetstreamTarget, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	return &jetstreamTarget{
		bin:  bin,
		addr: fmt.Sprintf("127.0.0.1:%d", port),
		url:  fmt.Sprintf("nats://127.0.0.1:%d", port),
		sync: sync,
	}, nil
}

func (j *jetstreamTarget) Name() string { return "jetstream" }

func (j *jetstreamTarget) Start(ctx context.Context, dir string) error {
	store := filepath.Join(dir, "jetstream")
	if err := os.MkdirAll(store, 0o755); err != nil {
		return err
	}
	// "always" fsyncs every write. The relaxed arm uses the server default so
	// that the fsync cost itself can be isolated and the durability match can
	// be shown to be real rather than asserted.
	interval := "always"
	if !j.sync {
		interval = "2m"
	}
	cfg := fmt.Sprintf(""+
		"listen: %q\n"+
		"logfile: %q\n"+
		"jetstream {\n"+
		"  store_dir: %q\n"+
		"  sync_interval: %q\n"+
		"}\n",
		j.addr, filepath.ToSlash(filepath.Join(dir, "nats.log")), filepath.ToSlash(store), interval)
	path := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		return err
	}

	cmd := exec.Command(j.bin, "-c", path)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start nats-server: %w", err)
	}
	j.cmd = cmd

	return waitReady(ctx, func() error {
		nc, err := nats.Connect(j.url, nats.Timeout(time.Second))
		if err != nil {
			return err
		}
		defer nc.Close()
		js, err := nc.JetStream()
		if err != nil {
			return err
		}
		// JetStream reports itself enabled only once its meta layer is up, so
		// this probe waits for the same readiness a client would.
		_, err = js.AccountInfo()
		return err
	})
}

func (j *jetstreamTarget) Setup(ctx context.Context) error {
	nc, err := nats.Connect(j.url)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "BENCH",
		Subjects:  []string{natsSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
	})
	return err
}

func (j *jetstreamTarget) NewPublisher() (Publisher, error) {
	nc, err := nats.Connect(j.url)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream(nats.MaxWait(30 * time.Second))
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &jetstreamPublisher{nc: nc, js: js}, nil
}

func (j *jetstreamTarget) Replay(ctx context.Context, want int) (int, error) {
	nc, err := nats.Connect(j.url)
	if err != nil {
		return 0, err
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(30 * time.Second))
	if err != nil {
		return 0, err
	}
	// An ordered consumer is a server-pushed, in-order replay from the start
	// of the stream, which is the same shape as a Pulse Subscribe with
	// Follow=false and Start=0.
	sub, err := js.SubscribeSync(natsSubject, nats.OrderedConsumer(), nats.DeliverAll())
	if err != nil {
		return 0, err
	}
	defer func() { _ = sub.Unsubscribe() }()

	seen := 0
	for seen < want {
		if _, err := sub.NextMsgWithContext(ctx); err != nil {
			return seen, err
		}
		seen++
	}
	return seen, nil
}

func (j *jetstreamTarget) Kill() error {
	if j.cmd == nil || j.cmd.Process == nil {
		return nil
	}
	if err := j.cmd.Process.Kill(); err != nil {
		return err
	}
	_ = j.cmd.Wait()
	j.cmd = nil
	time.Sleep(200 * time.Millisecond)
	return nil
}

type jetstreamPublisher struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

func (p *jetstreamPublisher) Publish(ctx context.Context, payload []byte) error {
	_, err := p.js.Publish(natsSubject, payload, nats.Context(ctx))
	return err
}

func (p *jetstreamPublisher) Close() error {
	p.nc.Close()
	return nil
}
