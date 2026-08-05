// Package config loads, validates, and resolves the broker configuration.
//
// Configuration comes from three sources, in increasing precedence: built-in
// defaults, an optional YAML file, and PULSE_* environment variables. The
// defaults and key names follow the frozen docs: the listen address and
// transport bounds come from docs/Protocol.md §6, the storage tunables
// (segment size, index interval, sync mode) from docs/Storage.md §5, and the
// engine defaults from the storage engine itself.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// Defaults for the broker configuration.
const (
	DefaultListenAddr      = "127.0.0.1:9090"
	DefaultMaxMsgBytes     = 64 << 20  // 64 MiB transport bound (Protocol.md §6)
	DefaultSegmentMaxBytes = 512 << 20 // 512 MiB (Storage.md §5)
	DefaultIndexInterval   = 4096      // 4 KiB (Storage.md §4)
	DefaultDataDir         = "data"
)

// DefaultRetentionInterval is how often the retention sweeper runs (Storage.md
// §5, §8).
const DefaultRetentionInterval = 30 * time.Second

// DefaultShutdownGrace is how long the server waits for graceful drain before
// force-closing in-flight RPCs (Concurrency.md §6).
const DefaultShutdownGrace = 10 * time.Second

// Config is the fully resolved broker configuration.
type Config struct {
	// ListenAddr is the address the gRPC server binds to.
	ListenAddr string `yaml:"listen-addr" json:"listen-addr"`
	// DataDir is the root of the data plane and metadata plane directories.
	DataDir string `yaml:"data-dir" json:"data-dir"`
	// MaxRecvMsgSize bounds a single received gRPC message in bytes.
	MaxRecvMsgSize int `yaml:"max-recv-msg-size" json:"max-recv-msg-size"`
	// MaxSendMsgSize bounds a single sent gRPC message in bytes.
	MaxSendMsgSize int `yaml:"max-send-msg-size" json:"max-send-msg-size"`
	// LogLevel is the zap log level: debug, info, warn, error.
	LogLevel string `yaml:"log-level" json:"log-level"`
	// Development enables human-readable logging output.
	Development bool `yaml:"development" json:"development"`
	// ShutdownGrace is the graceful-stop drain timeout.
	ShutdownGrace Duration `yaml:"shutdown-grace" json:"shutdown-grace"`
	// Storage carries the data-plane tunables (Storage.md §4-5).
	Storage Storage `yaml:"storage" json:"storage"`
	// Subscribe bounds a single subscribe read.
	Subscribe Subscribe `yaml:"subscribe" json:"subscribe"`
}

// Storage is the data-plane configuration.
type Storage struct {
	// SegmentMaxBytes rotates the active segment past this many bytes.
	SegmentMaxBytes int64 `yaml:"segment-max-bytes" json:"segment-max-bytes"`
	// IndexIntervalBytes adds a sparse index entry per this many bytes.
	IndexIntervalBytes int64 `yaml:"index-interval-bytes" json:"index-interval-bytes"`
	// SyncMode is "every-write" (strict durability) or "interval".
	SyncMode string `yaml:"sync-mode" json:"sync-mode"`
	// SyncInterval is the fsync period for interval mode.
	SyncInterval Duration `yaml:"sync-interval" json:"sync-interval"`
	// RetentionInterval is how often the retention sweeper runs; zero disables
	// the background sweeper.
	RetentionInterval Duration `yaml:"retention-interval" json:"retention-interval"`
}

// Subscribe bounds a single subscribe read.
type Subscribe struct {
	// ReadLimit caps records returned per read.
	ReadLimit int `yaml:"read-limit" json:"read-limit"`
	// ReadMaxBytes caps payload bytes returned per read.
	ReadMaxBytes int `yaml:"read-max-bytes" json:"read-max-bytes"`
}

// Default returns the built-in configuration defaults.
func Default() Config {
	return Config{
		ListenAddr:     DefaultListenAddr,
		DataDir:        DefaultDataDir,
		MaxRecvMsgSize: DefaultMaxMsgBytes,
		MaxSendMsgSize: DefaultMaxMsgBytes,
		LogLevel:       "info",
		ShutdownGrace:  Duration(DefaultShutdownGrace),
		Storage: Storage{
			SegmentMaxBytes:    DefaultSegmentMaxBytes,
			IndexIntervalBytes: DefaultIndexInterval,
			SyncMode:           "every-write",
			SyncInterval:       Duration(100 * time.Millisecond),
			RetentionInterval:  Duration(DefaultRetentionInterval),
		},
		Subscribe: Subscribe{
			ReadLimit:    512,
			ReadMaxBytes: 1 << 20, // 1 MiB
		},
	}
}

// Load resolves the configuration: defaults, then the YAML file at path (if
// non-empty), then PULSE_* environment overrides, then validation.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	cfg.applyEnv()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyEnv applies PULSE_* environment overrides.
func (c *Config) applyEnv() {
	if v := os.Getenv("PULSE_LISTEN_ADDR"); v != "" {
		c.ListenAddr = v
	}
	if v := os.Getenv("PULSE_DATA_DIR"); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv("PULSE_LOG_LEVEL"); v != "" {
		c.LogLevel = v
	}
	if v := os.Getenv("PULSE_SYNC_MODE"); v != "" {
		c.Storage.SyncMode = v
	}
}

// Validate checks the configuration against its limits.
func (c Config) Validate() error {
	var errs []error
	if c.ListenAddr == "" {
		errs = append(errs, errors.New("listen-addr must not be empty"))
	}
	if c.DataDir == "" {
		errs = append(errs, errors.New("data-dir must not be empty"))
	}
	if c.MaxRecvMsgSize <= 0 || c.MaxRecvMsgSize > 256<<20 {
		errs = append(errs, fmt.Errorf("max-recv-msg-size %d out of range", c.MaxRecvMsgSize))
	}
	if c.MaxSendMsgSize <= 0 || c.MaxSendMsgSize > 256<<20 {
		errs = append(errs, fmt.Errorf("max-send-msg-size %d out of range", c.MaxSendMsgSize))
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log-level %q must be debug|info|warn|error", c.LogLevel))
	}
	if c.ShutdownGrace < 0 {
		errs = append(errs, errors.New("shutdown-grace must not be negative"))
	}
	if c.Storage.SegmentMaxBytes <= 0 || c.Storage.SegmentMaxBytes > 1<<31 {
		errs = append(errs, fmt.Errorf("storage.segment-max-bytes %d out of range", c.Storage.SegmentMaxBytes))
	}
	if c.Storage.IndexIntervalBytes <= 0 {
		errs = append(errs, errors.New("storage.index-interval-bytes must be positive"))
	}
	switch c.Storage.SyncMode {
	case "every-write", "interval":
	default:
		errs = append(errs, fmt.Errorf("storage.sync-mode %q must be every-write|interval", c.Storage.SyncMode))
	}
	if c.Storage.SyncInterval <= 0 {
		errs = append(errs, errors.New("storage.sync-interval must be positive"))
	}
	if c.Storage.RetentionInterval < 0 {
		errs = append(errs, errors.New("storage.retention-interval must not be negative"))
	}
	if c.Subscribe.ReadLimit <= 0 {
		errs = append(errs, errors.New("subscribe.read-limit must be positive"))
	}
	if c.Subscribe.ReadMaxBytes <= 0 {
		errs = append(errs, errors.New("subscribe.read-max-bytes must be positive"))
	}
	return errors.Join(errs...)
}

// Duration is a time.Duration that unmarshals from a YAML/JSON string like
// "100ms" or "10s".
type Duration time.Duration

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// UnmarshalJSON implements json.Unmarshaler for the JSON-marshaled Config.
func (d *Duration) UnmarshalJSON(data []byte) error {
	s := string(data)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON implements json.Marshaler so durations round-trip as strings.
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", time.Duration(d).String())), nil
}
