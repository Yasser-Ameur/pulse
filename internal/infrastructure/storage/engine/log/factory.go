package log

import (
	"context"

	"github.com/Yasser-Ameur/pulse/internal/application/ports"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/storage"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

// Factory is the ports.LogFactory implementation backed by the segment log
// engine. It resolves partition directories under a single data root.
type Factory struct {
	root   string
	cfg    Config
	logger ports.Logger
}

// NewFactory builds a log factory rooted at dataRoot.
func NewFactory(dataRoot string, cfg Config, logger ports.Logger) *Factory {
	return &Factory{root: dataRoot, cfg: cfg.ApplyDefaults(), logger: logger}
}

// Create makes a new empty log for the partition.
func (f *Factory) Create(_ context.Context, name topic.Name, id partition.ID) (storage.Log, error) {
	return CreateLog(f.root, name, id, f.cfg, f.logger)
}

// Open opens the existing log for the partition. OpenLog wraps
// ports.ErrLogNotFound when the partition has no data on disk.
func (f *Factory) Open(_ context.Context, name topic.Name, id partition.ID) (storage.Log, error) {
	return OpenLog(f.root, name, id, f.cfg, f.logger)
}

// Delete removes the partition log and all of its data.
func (f *Factory) Delete(_ context.Context, name topic.Name, id partition.ID) error {
	return DeleteLog(f.root, name, id)
}

// Config exposes the factory's resolved configuration.
func (f *Factory) Config() Config { return f.cfg }
