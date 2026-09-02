package services

import (
	"context"
	"errors"

	"github.com/Yasser-Ameur/pulse/internal/application/ports"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

// TopicManager coordinates topic lifecycle across the metadata store and the
// log factory. The metadata store is the source of truth: recovery recreates
// missing logs, so a metadata commit precedes log creation and cleanup follows
// any partial failure.
type TopicManager struct {
	store    ports.MetadataStore
	factory  ports.LogFactory
	registry *LogRegistry
	clock    ports.Clock
	logger   ports.Logger
}

// NewTopicManager builds a TopicManager over the given dependencies.
func NewTopicManager(store ports.MetadataStore, factory ports.LogFactory, registry *LogRegistry, clock ports.Clock, logger ports.Logger) *TopicManager {
	return &TopicManager{
		store:    store,
		factory:  factory,
		registry: registry,
		clock:    clock,
		logger:   logger,
	}
}

// CreateTopic validates the name and configuration, persists the definition,
// and opens the partition logs. Partition counts from 1 to topic.MaxPartitions
// are accepted.
func (m *TopicManager) CreateTopic(ctx context.Context, name string, cfg topic.Config, partitions int) (topic.Topic, error) {
	n, err := topic.NewName(name)
	if err != nil {
		return topic.Topic{}, err
	}
	cfg, err = cfg.Validate()
	if err != nil {
		return topic.Topic{}, err
	}
	if partitions < 1 || partitions > topic.MaxPartitions {
		return topic.Topic{}, topic.ErrInvalidPartitionCount
	}
	if _, ok, err := m.store.GetTopic(ctx, n); err != nil {
		return topic.Topic{}, err
	} else if ok {
		return topic.Topic{}, topic.ErrAlreadyExists
	}

	t := topic.Topic{
		Name:       n,
		Partitions: partitions,
		Config:     cfg,
		CreatedAt:  m.clock.Now(),
	}
	if err := m.store.CreateTopic(ctx, t); err != nil {
		return topic.Topic{}, err
	}

	var created []storageLogRef
	for p := 0; p < partitions; p++ {
		pid := partition.ID(p)
		lg, err := m.factory.Create(ctx, n, pid)
		if err != nil {
			m.rollbackCreate(ctx, t, created)
			return topic.Topic{}, err
		}
		m.registry.RegisterLog(n, pid, lg)
		created = append(created, storageLogRef{name: n, partition: pid})
	}
	m.registry.RegisterTopic(t)
	m.logger.Info("topic created",
		ports.Field{Key: logKeyTopic, Value: t.Name.String()},
		ports.Field{Key: "partitions", Value: t.Partitions},
	)
	return t, nil
}

// DeleteTopic removes the topic definition, drops it from the registry, and
// deletes its partition logs. It returns topic.ErrNotFound if the topic does
// not exist.
func (m *TopicManager) DeleteTopic(ctx context.Context, name string) error {
	n, err := topic.NewName(name)
	if err != nil {
		return err
	}
	if err := m.store.DeleteTopic(ctx, n); err != nil {
		return err
	}
	logs := m.registry.RemoveTopic(n)
	var errs []error
	for _, pl := range logs {
		if err := pl.log.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := m.factory.Delete(ctx, n, pl.partition); err != nil {
			errs = append(errs, err)
		}
	}
	m.logger.Info("topic deleted", ports.Field{Key: logKeyTopic, Value: n.String()})
	return errors.Join(errs...)
}

// ListTopics returns the live topic definitions in name order.
func (m *TopicManager) ListTopics(ctx context.Context) ([]topic.Topic, error) {
	return m.registry.Topics(), nil
}

// rollbackCreate removes the logs and metadata of a partially created topic.
func (m *TopicManager) rollbackCreate(ctx context.Context, t topic.Topic, created []storageLogRef) {
	for _, ref := range created {
		m.registry.UnregisterLog(ref.name, ref.partition)
		if err := m.factory.Delete(ctx, ref.name, ref.partition); err != nil {
			m.logger.Error("failed to roll back partition log",
				ports.Field{Key: logKeyTopic, Value: t.Name.String()},
				ports.Field{Key: logKeyPartition, Value: ref.partition.Int32()},
				ports.Field{Key: logKeyError, Value: err},
			)
		}
	}
	if err := m.store.DeleteTopic(ctx, t.Name); err != nil {
		m.logger.Error("failed to roll back topic metadata",
			ports.Field{Key: logKeyTopic, Value: t.Name.String()},
			ports.Field{Key: "error", Value: err},
		)
	}
}

// storageLogRef records a created partition log for rollback bookkeeping.
type storageLogRef struct {
	name      topic.Name
	partition partition.ID
}
