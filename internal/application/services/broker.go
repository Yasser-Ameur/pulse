package services

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/domain/broker"
	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/storage"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

// Broker is the application facade: it owns the broker lifecycle, identity,
// and the delegation of every operation to the specialized services. It is the
// single entry point used by the gRPC adapter.
type Broker struct {
	mu         sync.Mutex
	state      broker.State
	identity   broker.Broker
	registry   *LogRegistry
	topics     *TopicManager
	publisher  *Publisher
	subscriber *Subscriber
	store      ports.MetadataStore
	factory    ports.LogFactory
	clock      ports.Clock
	logger     ports.Logger
	version    string

	retentionInterval time.Duration
	sweepStop         chan struct{}

	// drainCtx is canceled with cause broker.ErrDraining the instant Shutdown
	// begins, so live Subscribe streams unblock immediately instead of
	// holding gRPC's GracefulStop for the whole grace window.
	drainCtx    context.Context
	drainCancel context.CancelCauseFunc

	// Stats counters, snapshotted by Stats(). Incremented at the same call
	// sites that already invoke MetricsRecorder.
	subscriptions    atomic.Int64
	publishedRecords atomic.Int64
	publishedBytes   atomic.Int64
	deliveredRecords atomic.Int64
	deliveredBytes   atomic.Int64
}

// Stats is a point-in-time snapshot of broker-wide counters, for the monitor
// HTTP listener's /varz endpoint.
type Stats struct {
	Subscriptions    int64
	PublishedRecords int64
	PublishedBytes   int64
	DeliveredRecords int64
	DeliveredBytes   int64
}

// Stats returns a snapshot of the broker's counters.
func (b *Broker) Stats() Stats {
	return Stats{
		Subscriptions:    b.subscriptions.Load(),
		PublishedRecords: b.publishedRecords.Load(),
		PublishedBytes:   b.publishedBytes.Load(),
		DeliveredRecords: b.deliveredRecords.Load(),
		DeliveredBytes:   b.deliveredBytes.Load(),
	}
}

// BrokerOptions wires a Broker to its infrastructure.
type BrokerOptions struct {
	MetadataStore ports.MetadataStore
	LogFactory    ports.LogFactory
	Clock         ports.Clock
	Logger        ports.Logger
	Metrics       ports.MetricsRecorder
	// ListenAddr is the address clients connect to; it is reported by
	// BrokerInfo.
	ListenAddr string
	// Version is the broker release version reported by BrokerInfo.
	Version string
	// ReadLimit bounds records per subscribe read.
	ReadLimit int
	// ReadMaxBytes bounds payload bytes per subscribe read.
	ReadMaxBytes int
	// RetentionInterval is how often the background retention sweeper runs.
	// Zero disables the sweeper.
	RetentionInterval time.Duration
}

// NewBroker constructs a Broker in the Starting state.
func NewBroker(opts BrokerOptions) *Broker {
	registry := NewLogRegistry()
	topics := NewTopicManager(opts.MetadataStore, opts.LogFactory, registry, opts.Clock, opts.Logger)
	b := &Broker{
		state:    broker.StateStarting,
		registry: registry,
		topics:   topics,
		store:    opts.MetadataStore,
		factory:  opts.LogFactory,
		clock:    opts.Clock,
		logger:   opts.Logger,
		version:  opts.Version,
		identity: broker.Broker{
			Address: opts.ListenAddr,
			State:   broker.StateStarting,
		},
		retentionInterval: opts.RetentionInterval,
	}
	b.publisher = NewPublisher(registry, opts.Clock, opts.Logger, opts.Metrics, &b.publishedRecords, &b.publishedBytes)
	b.subscriber = NewSubscriber(registry, opts.MetadataStore, SubscriberOptions{
		ReadLimit:    opts.ReadLimit,
		ReadMaxBytes: opts.ReadMaxBytes,
	}, opts.Logger, opts.Metrics, &b.deliveredRecords, &b.deliveredBytes, &b.subscriptions)
	return b
}

// Start drives the broker through identity establishment and log recovery into
// the Running state.
func (b *Broker) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.state.CanTransitionTo(broker.StateRecovering) {
		return broker.ErrInvalidTransition
	}
	b.state = broker.StateRecovering
	b.drainCtx, b.drainCancel = context.WithCancelCause(context.Background())

	now := b.clock.Now()
	clusterID, ok, err := b.store.ClusterID(ctx)
	if err != nil {
		return err
	}
	if !ok {
		clusterID = broker.NewClusterID(now)
		if err := b.store.CreateCluster(ctx, clusterID); err != nil {
			return err
		}
	}
	id, ok, err := b.store.BrokerID(ctx)
	if err != nil {
		return err
	}
	if !ok {
		id = broker.NewBrokerID(now)
		if err := b.store.CreateBroker(ctx, id); err != nil {
			return err
		}
	}
	b.identity.ClusterID = clusterID
	b.identity.BrokerID = id
	b.identity.NodeID = broker.NodeID(id) // single-node deployments share node and broker id
	b.identity.Version = b.version

	topics, err := b.store.ListTopics(ctx)
	if err != nil {
		return err
	}
	var opened []storage.Log
	for _, t := range topics {
		for p := 0; p < t.Partitions; p++ {
			pid := partition.ID(p)
			lg, err := b.factory.Open(ctx, t.Name, pid)
			if err != nil {
				if errors.Is(err, ports.ErrLogNotFound) {
					// Metadata without storage: recreate the partition log.
					lg, err = b.factory.Create(ctx, t.Name, pid)
				}
				if err != nil {
					b.closeOpened(opened)
					return err
				}
			}
			b.registry.RegisterLog(t.Name, pid, lg)
			opened = append(opened, lg)
		}
		b.registry.RegisterTopic(t)
	}
	b.identity.StartedAt = b.clock.Now()
	b.state = broker.StateRunning
	b.logger.Info("broker started",
		ports.Field{Key: "cluster_id", Value: string(clusterID)},
		ports.Field{Key: "broker_id", Value: string(id)},
		ports.Field{Key: "address", Value: b.identity.Address},
		ports.Field{Key: "topics", Value: len(topics)},
	)
	b.startSweeper()
	return nil
}

// Drain moves a Running broker to Draining: new work is rejected with
// broker.ErrDraining and live Subscribe streams are canceled, while the logs
// stay open so in-flight publishes can finish. The composition root calls it
// before the transport's GracefulStop so followers do not hold the grace
// window; Shutdown repeats it, so skipping Drain is safe. Idempotent.
func (b *Broker) Drain() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.drainLocked()
}

func (b *Broker) drainLocked() {
	if b.state == broker.StateRunning {
		b.state = broker.StateDraining
	}
	if b.drainCancel != nil {
		b.drainCancel(broker.ErrDraining)
	}
}

// Shutdown drains the broker, flushes and closes all logs, closes the metadata
// store, and reaches the Stopped state. It is idempotent.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == broker.StateStopped {
		return nil
	}
	b.drainLocked()
	if b.state.CanTransitionTo(broker.StateStopping) {
		b.state = broker.StateStopping
	}
	b.stopSweeper()

	var errs []error
	for _, lg := range b.registry.Logs() {
		if err := lg.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := lg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := b.store.Close(); err != nil {
		errs = append(errs, err)
	}
	b.state = broker.StateStopped
	b.logger.Info("broker stopped")
	return errors.Join(errs...)
}

// State returns the current broker lifecycle state.
func (b *Broker) State() broker.State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// BrokerInfo returns the broker identity with the current lifecycle state.
func (b *Broker) BrokerInfo() broker.Broker {
	b.mu.Lock()
	defer b.mu.Unlock()
	info := b.identity
	info.State = b.state
	return info
}

// Publish validates and durably appends messages to a topic partition,
// returning the assigned offsets aligned with the input messages.
func (b *Broker) Publish(ctx context.Context, name topic.Name, id partition.ID, msgs []message.Message) ([]offset.Offset, error) {
	if err := b.running(); err != nil {
		return nil, err
	}
	t, ok := b.registry.Topic(name)
	if !ok {
		return nil, topic.ErrNotFound
	}
	if !id.Valid(t.Partitions) {
		return nil, partition.ErrNotFound
	}
	return b.publisher.Publish(ctx, t, id, msgs)
}

// Subscribe streams records for sub to emit. The stream is also canceled with
// cause broker.ErrDraining the instant Shutdown begins, so it does not hold
// gRPC's GracefulStop for the whole grace window.
func (b *Broker) Subscribe(ctx context.Context, sub consumer.Subscription, emit func([]message.Record) error) error {
	if err := b.running(); err != nil {
		return err
	}
	drainCtx := b.drainCtx
	merged, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := context.AfterFunc(drainCtx, func() { cancel(context.Cause(drainCtx)) })
	defer stop()
	return b.subscriber.Subscribe(merged, sub, emit)
}

// Ack advances a consumer's stored cursor.
func (b *Broker) Ack(ctx context.Context, c consumer.ID, name topic.Name, id partition.ID, o offset.Offset) (offset.Offset, error) {
	if err := b.running(); err != nil {
		return offset.Invalid, err
	}
	return b.subscriber.Ack(ctx, c, name, id, o)
}

// CreateTopic delegates to the topic manager.
func (b *Broker) CreateTopic(ctx context.Context, name string, cfg topic.Config, partitions int) (topic.Topic, error) {
	if err := b.running(); err != nil {
		return topic.Topic{}, err
	}
	return b.topics.CreateTopic(ctx, name, cfg, partitions)
}

// DeleteTopic delegates to the topic manager.
func (b *Broker) DeleteTopic(ctx context.Context, name string) error {
	if err := b.running(); err != nil {
		return err
	}
	return b.topics.DeleteTopic(ctx, name)
}

// ListTopics delegates to the topic manager.
func (b *Broker) ListTopics(ctx context.Context) ([]topic.Topic, error) {
	if err := b.running(); err != nil {
		return nil, err
	}
	return b.topics.ListTopics(ctx)
}

// running returns broker.ErrDraining while the broker is shutting down and
// broker.ErrNotRunning otherwise, unless the broker is serving requests.
func (b *Broker) running() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case broker.StateRunning:
		return nil
	case broker.StateDraining, broker.StateStopping:
		return broker.ErrDraining
	default:
		return broker.ErrNotRunning
	}
}

// startSweeper launches the background retention sweeper. The caller holds
// b.mu; the initial sweep runs asynchronously in the goroutine.
func (b *Broker) startSweeper() {
	if b.retentionInterval <= 0 || b.sweepStop != nil {
		return
	}
	b.sweepStop = make(chan struct{})
	stop := b.sweepStop
	go func() {
		b.sweep()
		t := time.NewTicker(b.retentionInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				b.sweep()
			case <-stop:
				return
			}
		}
	}()
}

// stopSweeper halts the background retention sweeper. The caller holds b.mu.
func (b *Broker) stopSweeper() {
	if b.sweepStop == nil {
		return
	}
	close(b.sweepStop)
	b.sweepStop = nil
}

// sweep applies each topic's retention policy to its partition logs. Topics
// with compaction cleanup or no retention limits are skipped. It is called by
// the background sweeper and is safe to call directly for tests.
func (b *Broker) sweep() {
	for _, t := range b.registry.Topics() {
		if t.Config.Cleanup != topic.CleanupDelete {
			continue
		}
		policy := t.Config.Retention
		if policy.MaxAge <= 0 && policy.MaxBytes <= 0 {
			continue
		}
		for p := 0; p < t.Partitions; p++ {
			pid := partition.ID(p)
			lg, ok := b.registry.Log(t.Name, pid)
			if !ok {
				continue
			}
			res, err := lg.Trim(b.clock.Now(), policy)
			if err != nil {
				b.logger.Warn("retention sweep failed",
					ports.Field{Key: logKeyTopic, Value: t.Name.String()},
					ports.Field{Key: logKeyPartition, Value: pid.Int32()},
					ports.Field{Key: logKeyError, Value: err.Error()},
				)
				continue
			}
			if res.Segments > 0 {
				b.logger.Info("retention swept",
					ports.Field{Key: logKeyTopic, Value: t.Name.String()},
					ports.Field{Key: logKeyPartition, Value: pid.Int32()},
					ports.Field{Key: "segments", Value: res.Segments},
					ports.Field{Key: "bytes", Value: res.Bytes},
				)
			}
		}
	}
}

// closeOpened closes every log opened so far during a failed startup.
func (b *Broker) closeOpened(logs []storage.Log) {
	for _, lg := range logs {
		_ = lg.Close()
	}
}
