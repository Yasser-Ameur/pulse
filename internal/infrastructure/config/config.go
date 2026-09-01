// Package config loads, validates, and resolves the broker configuration.
//
// Configuration comes from three sources, in increasing precedence: built-in
// defaults, an optional YAML file, and PULSE_* environment variables. The
// defaults and key names follow the frozen docs: the listen address and
// transport bounds come from docs/Protocol.md §6, the storage tunables
// (segment size, index interval, sync mode) from docs/Storage.md §5, and the
// engine defaults from the storage engine itself.
//
// Every YAML key has a matching PULSE_<UPPER_SNAKE> environment override,
// named after its YAML path with dots replaced by underscores (e.g.
// storage.sync-interval becomes PULSE_STORAGE_SYNC_INTERVAL). The original
// four override names (PULSE_LISTEN_ADDR, PULSE_DATA_DIR, PULSE_LOG_LEVEL,
// PULSE_SYNC_MODE) already matched or predate this rule and are kept for
// compatibility; PULSE_SYNC_MODE is still accepted alongside the
// rule-conforming PULSE_STORAGE_SYNC_MODE.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
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
	// DefaultMonitorAddr is the address the monitor HTTP listener (health,
	// readiness, varz, metrics) binds to. An empty monitor-addr disables it.
	DefaultMonitorAddr = "127.0.0.1:9091"
)

// Storage sync modes.
const (
	SyncModeEveryWrite = "every-write"
	SyncModeInterval   = "interval"
)

// Log levels accepted by LogLevel.
const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
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
	// MonitorAddr is the address the monitor HTTP listener (health,
	// readiness, varz, metrics) binds to. Empty disables the listener.
	MonitorAddr string `yaml:"monitor-addr" json:"monitor-addr"`
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
		LogLevel:       LogLevelInfo,
		ShutdownGrace:  Duration(DefaultShutdownGrace),
		MonitorAddr:    DefaultMonitorAddr,
		Storage: Storage{
			SegmentMaxBytes:    DefaultSegmentMaxBytes,
			IndexIntervalBytes: DefaultIndexInterval,
			SyncMode:           SyncModeEveryWrite,
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
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	if err := cfg.applyEnv(); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// envString applies a string environment override to dst if key is set to a
// non-empty value.
func envString(key string, dst *string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}

// envBool applies a bool environment override to dst, appending a parse error
// naming key to errs on failure.
func envBool(key string, dst *bool, errs *[]error) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("env %s: %w", key, err))
		return
	}
	*dst = b
}

// envInt applies an int environment override to dst, appending a parse error
// naming key to errs on failure.
func envInt(key string, dst *int, errs *[]error) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("env %s: %w", key, err))
		return
	}
	*dst = n
}

// envInt64 applies an int64 environment override to dst, appending a parse
// error naming key to errs on failure.
func envInt64(key string, dst *int64, errs *[]error) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("env %s: %w", key, err))
		return
	}
	*dst = n
}

// envDuration applies a Duration environment override to dst, appending a
// parse error naming key to errs on failure.
func envDuration(key string, dst *Duration, errs *[]error) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("env %s: %w", key, err))
		return
	}
	*dst = Duration(d)
}

// applyEnv applies PULSE_* environment overrides. Every YAML key has an
// override named PULSE_<UPPER_SNAKE path> (dots become underscores); a
// malformed value for a typed key is reported as an error naming the
// variable.
func (c *Config) applyEnv() error {
	var errs []error

	envString("PULSE_LISTEN_ADDR", &c.ListenAddr)
	envString("PULSE_DATA_DIR", &c.DataDir)
	envInt("PULSE_MAX_RECV_MSG_SIZE", &c.MaxRecvMsgSize, &errs)
	envInt("PULSE_MAX_SEND_MSG_SIZE", &c.MaxSendMsgSize, &errs)
	envString("PULSE_LOG_LEVEL", &c.LogLevel)
	envBool("PULSE_DEVELOPMENT", &c.Development, &errs)
	envDuration("PULSE_SHUTDOWN_GRACE", &c.ShutdownGrace, &errs)
	envString("PULSE_MONITOR_ADDR", &c.MonitorAddr)

	envInt64("PULSE_STORAGE_SEGMENT_MAX_BYTES", &c.Storage.SegmentMaxBytes, &errs)
	envInt64("PULSE_STORAGE_INDEX_INTERVAL_BYTES", &c.Storage.IndexIntervalBytes, &errs)
	// PULSE_SYNC_MODE is the legacy override name; PULSE_STORAGE_SYNC_MODE is
	// the rule-conforming name and wins when both are set.
	envString("PULSE_SYNC_MODE", &c.Storage.SyncMode)
	envString("PULSE_STORAGE_SYNC_MODE", &c.Storage.SyncMode)
	envDuration("PULSE_STORAGE_SYNC_INTERVAL", &c.Storage.SyncInterval, &errs)
	envDuration("PULSE_STORAGE_RETENTION_INTERVAL", &c.Storage.RetentionInterval, &errs)

	envInt("PULSE_SUBSCRIBE_READ_LIMIT", &c.Subscribe.ReadLimit, &errs)
	envInt("PULSE_SUBSCRIBE_READ_MAX_BYTES", &c.Subscribe.ReadMaxBytes, &errs)

	return errors.Join(errs...)
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
	case LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError:
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
	case SyncModeEveryWrite, SyncModeInterval:
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
