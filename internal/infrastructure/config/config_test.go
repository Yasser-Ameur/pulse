package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// writeConfig writes contents to a temp YAML file and returns its path.
func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pulse.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}

// clearEnv unsets every PULSE_* variable applyEnv reads, so a test starts from
// a known environment regardless of the developer's shell.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"PULSE_LISTEN_ADDR",
		"PULSE_DATA_DIR",
		"PULSE_LOG_LEVEL",
		"PULSE_SYNC_MODE",
	} {
		t.Setenv(k, "")
	}
}

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("Default().Validate() error = %v, want nil", err)
	}
}

func TestDefaultValues(t *testing.T) {
	cfg := Default()
	if cfg.ListenAddr != DefaultListenAddr {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, DefaultListenAddr)
	}
	if cfg.DataDir != DefaultDataDir {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, DefaultDataDir)
	}
	if cfg.MaxRecvMsgSize != DefaultMaxMsgBytes {
		t.Errorf("MaxRecvMsgSize = %d, want %d", cfg.MaxRecvMsgSize, DefaultMaxMsgBytes)
	}
	if cfg.MaxSendMsgSize != DefaultMaxMsgBytes {
		t.Errorf("MaxSendMsgSize = %d, want %d", cfg.MaxSendMsgSize, DefaultMaxMsgBytes)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.Development {
		t.Error("Development = true, want false")
	}
	if cfg.ShutdownGrace.Duration() != DefaultShutdownGrace {
		t.Errorf("ShutdownGrace = %v, want %v", cfg.ShutdownGrace.Duration(), DefaultShutdownGrace)
	}
	if cfg.Storage.SegmentMaxBytes != DefaultSegmentMaxBytes {
		t.Errorf("SegmentMaxBytes = %d, want %d", cfg.Storage.SegmentMaxBytes, DefaultSegmentMaxBytes)
	}
	if cfg.Storage.IndexIntervalBytes != DefaultIndexInterval {
		t.Errorf("IndexIntervalBytes = %d, want %d", cfg.Storage.IndexIntervalBytes, DefaultIndexInterval)
	}
	if cfg.Storage.SyncMode != "every-write" {
		t.Errorf("SyncMode = %q, want %q", cfg.Storage.SyncMode, "every-write")
	}
	if cfg.Storage.RetentionInterval.Duration() != DefaultRetentionInterval {
		t.Errorf("RetentionInterval = %v, want %v", cfg.Storage.RetentionInterval.Duration(), DefaultRetentionInterval)
	}
	if cfg.Subscribe.ReadLimit != 512 {
		t.Errorf("Subscribe.ReadLimit = %d, want 512", cfg.Subscribe.ReadLimit)
	}
	if cfg.Subscribe.ReadMaxBytes != 1<<20 {
		t.Errorf("Subscribe.ReadMaxBytes = %d, want %d", cfg.Subscribe.ReadMaxBytes, 1<<20)
	}
}

func TestLoadEmptyPathReturnsDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") error = %v", err)
	}
	if cfg != Default() {
		t.Errorf("Load(\"\") = %+v, want %+v", cfg, Default())
	}
}

func TestLoadYAMLOverridesDefaults(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, `
listen-addr: "0.0.0.0:7000"
data-dir: "/var/lib/pulse"
max-recv-msg-size: 1048576
log-level: "debug"
development: true
shutdown-grace: "5s"
storage:
  segment-max-bytes: 1024
  index-interval-bytes: 128
  sync-mode: "interval"
  sync-interval: "250ms"
  retention-interval: "1m"
subscribe:
  read-limit: 10
  read-max-bytes: 2048
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr != "0.0.0.0:7000" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, "0.0.0.0:7000")
	}
	if cfg.DataDir != "/var/lib/pulse" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/var/lib/pulse")
	}
	if cfg.MaxRecvMsgSize != 1<<20 {
		t.Errorf("MaxRecvMsgSize = %d, want %d", cfg.MaxRecvMsgSize, 1<<20)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "debug")
	}
	if !cfg.Development {
		t.Error("Development = false, want true")
	}
	if cfg.ShutdownGrace.Duration() != 5*time.Second {
		t.Errorf("ShutdownGrace = %v, want 5s", cfg.ShutdownGrace.Duration())
	}
	if cfg.Storage.SyncMode != "interval" {
		t.Errorf("SyncMode = %q, want %q", cfg.Storage.SyncMode, "interval")
	}
	if cfg.Storage.SyncInterval.Duration() != 250*time.Millisecond {
		t.Errorf("SyncInterval = %v, want 250ms", cfg.Storage.SyncInterval.Duration())
	}
	if cfg.Storage.RetentionInterval.Duration() != time.Minute {
		t.Errorf("RetentionInterval = %v, want 1m", cfg.Storage.RetentionInterval.Duration())
	}
	if cfg.Subscribe.ReadLimit != 10 {
		t.Errorf("Subscribe.ReadLimit = %d, want 10", cfg.Subscribe.ReadLimit)
	}
	// A key absent from the file keeps its default rather than zeroing.
	if cfg.MaxSendMsgSize != DefaultMaxMsgBytes {
		t.Errorf("MaxSendMsgSize = %d, want default %d", cfg.MaxSendMsgSize, DefaultMaxMsgBytes)
	}
}

func TestLoadMissingFile(t *testing.T) {
	clearEnv(t)
	path := filepath.Join(t.TempDir(), "does-not-exist.yaml")
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want a read error")
	}
	if !strings.Contains(err.Error(), "read config") {
		t.Errorf("Load() error = %v, want it to mention \"read config\"", err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Load() error = %v, want it to wrap os.ErrNotExist", err)
	}
}

func TestLoadMalformedYAML(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, "listen-addr: [unclosed\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want a parse error")
	}
	if !strings.Contains(err.Error(), "parse config") {
		t.Errorf("Load() error = %v, want it to mention \"parse config\"", err)
	}
}

func TestLoadInvalidDurationInYAML(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, "shutdown-grace: \"not-a-duration\"\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want a parse error")
	}
	if !strings.Contains(err.Error(), "parse config") {
		t.Errorf("Load() error = %v, want it to mention \"parse config\"", err)
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, "log-level: \"trace\"\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want a validation error")
	}
	if !strings.Contains(err.Error(), "log-level") {
		t.Errorf("Load() error = %v, want it to mention log-level", err)
	}
	// A failed Load returns the zero Config, not a partially resolved one.
	cfg, _ := Load(path)
	if cfg != (Config{}) {
		t.Errorf("Load() config = %+v, want zero Config on error", cfg)
	}
}

func TestLoadEnvOverridesDefaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("PULSE_LISTEN_ADDR", "10.0.0.1:1234")
	t.Setenv("PULSE_DATA_DIR", "/env/data")
	t.Setenv("PULSE_LOG_LEVEL", "warn")
	t.Setenv("PULSE_SYNC_MODE", "interval")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr != "10.0.0.1:1234" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, "10.0.0.1:1234")
	}
	if cfg.DataDir != "/env/data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/env/data")
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "warn")
	}
	if cfg.Storage.SyncMode != "interval" {
		t.Errorf("SyncMode = %q, want %q", cfg.Storage.SyncMode, "interval")
	}
}

// TestLoadEnvBeatsYAML pins the documented precedence: defaults, then file,
// then environment.
func TestLoadEnvBeatsYAML(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, `
listen-addr: "file:1"
data-dir: "file-data"
log-level: "debug"
storage:
  sync-mode: "every-write"
`)
	t.Setenv("PULSE_LISTEN_ADDR", "env:2")
	t.Setenv("PULSE_DATA_DIR", "env-data")
	t.Setenv("PULSE_LOG_LEVEL", "error")
	t.Setenv("PULSE_SYNC_MODE", "interval")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr != "env:2" {
		t.Errorf("ListenAddr = %q, want env to win with %q", cfg.ListenAddr, "env:2")
	}
	if cfg.DataDir != "env-data" {
		t.Errorf("DataDir = %q, want env to win with %q", cfg.DataDir, "env-data")
	}
	if cfg.LogLevel != "error" {
		t.Errorf("LogLevel = %q, want env to win with %q", cfg.LogLevel, "error")
	}
	if cfg.Storage.SyncMode != "interval" {
		t.Errorf("SyncMode = %q, want env to win with %q", cfg.Storage.SyncMode, "interval")
	}
}

// TestLoadEmptyEnvDoesNotOverride pins that an exported-but-empty variable is
// treated as unset, so it cannot blank out a configured value.
func TestLoadEmptyEnvDoesNotOverride(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, "listen-addr: \"file:1\"\nlog-level: \"debug\"\n")
	t.Setenv("PULSE_LISTEN_ADDR", "")
	t.Setenv("PULSE_LOG_LEVEL", "")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr != "file:1" {
		t.Errorf("ListenAddr = %q, want the file value %q", cfg.ListenAddr, "file:1")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want the file value %q", cfg.LogLevel, "debug")
	}
}

// TestLoadEnvCanProduceInvalidConfig pins that environment overrides are
// validated too, not trusted because they came from the operator.
func TestLoadEnvCanProduceInvalidConfig(t *testing.T) {
	clearEnv(t)
	t.Setenv("PULSE_SYNC_MODE", "sometimes")
	_, err := Load("")
	if err == nil {
		t.Fatal("Load() error = nil, want a validation error")
	}
	if !strings.Contains(err.Error(), "sync-mode") {
		t.Errorf("Load() error = %v, want it to mention sync-mode", err)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // substring; empty means the config must validate
	}{
		{name: "default", mutate: func(*Config) {}},
		{
			name:    "empty listen addr",
			mutate:  func(c *Config) { c.ListenAddr = "" },
			wantErr: "listen-addr must not be empty",
		},
		{
			name:    "empty data dir",
			mutate:  func(c *Config) { c.DataDir = "" },
			wantErr: "data-dir must not be empty",
		},
		{
			name:    "zero max recv",
			mutate:  func(c *Config) { c.MaxRecvMsgSize = 0 },
			wantErr: "max-recv-msg-size 0 out of range",
		},
		{
			name:    "negative max recv",
			mutate:  func(c *Config) { c.MaxRecvMsgSize = -1 },
			wantErr: "max-recv-msg-size -1 out of range",
		},
		{
			name:    "max recv above cap",
			mutate:  func(c *Config) { c.MaxRecvMsgSize = (256 << 20) + 1 },
			wantErr: "max-recv-msg-size",
		},
		{
			name:   "max recv at cap",
			mutate: func(c *Config) { c.MaxRecvMsgSize = 256 << 20 },
		},
		{
			name:    "zero max send",
			mutate:  func(c *Config) { c.MaxSendMsgSize = 0 },
			wantErr: "max-send-msg-size 0 out of range",
		},
		{
			name:    "max send above cap",
			mutate:  func(c *Config) { c.MaxSendMsgSize = (256 << 20) + 1 },
			wantErr: "max-send-msg-size",
		},
		{
			name:   "max send at cap",
			mutate: func(c *Config) { c.MaxSendMsgSize = 256 << 20 },
		},
		{
			name:    "unknown log level",
			mutate:  func(c *Config) { c.LogLevel = "trace" },
			wantErr: `log-level "trace" must be debug|info|warn|error`,
		},
		{
			name:    "empty log level",
			mutate:  func(c *Config) { c.LogLevel = "" },
			wantErr: "log-level",
		},
		{name: "log level debug", mutate: func(c *Config) { c.LogLevel = "debug" }},
		{name: "log level warn", mutate: func(c *Config) { c.LogLevel = "warn" }},
		{name: "log level error", mutate: func(c *Config) { c.LogLevel = "error" }},
		{
			name:    "negative shutdown grace",
			mutate:  func(c *Config) { c.ShutdownGrace = Duration(-time.Second) },
			wantErr: "shutdown-grace must not be negative",
		},
		{
			name:   "zero shutdown grace allowed",
			mutate: func(c *Config) { c.ShutdownGrace = 0 },
		},
		{
			name:    "zero segment max bytes",
			mutate:  func(c *Config) { c.Storage.SegmentMaxBytes = 0 },
			wantErr: "storage.segment-max-bytes 0 out of range",
		},
		{
			name:    "segment max bytes above cap",
			mutate:  func(c *Config) { c.Storage.SegmentMaxBytes = (1 << 31) + 1 },
			wantErr: "storage.segment-max-bytes",
		},
		{
			name:   "segment max bytes at cap",
			mutate: func(c *Config) { c.Storage.SegmentMaxBytes = 1 << 31 },
		},
		{
			name:    "zero index interval",
			mutate:  func(c *Config) { c.Storage.IndexIntervalBytes = 0 },
			wantErr: "storage.index-interval-bytes must be positive",
		},
		{
			name:    "negative index interval",
			mutate:  func(c *Config) { c.Storage.IndexIntervalBytes = -8 },
			wantErr: "storage.index-interval-bytes must be positive",
		},
		{
			name:    "unknown sync mode",
			mutate:  func(c *Config) { c.Storage.SyncMode = "maybe" },
			wantErr: `storage.sync-mode "maybe" must be every-write|interval`,
		},
		{
			name:   "sync mode interval",
			mutate: func(c *Config) { c.Storage.SyncMode = "interval" },
		},
		{
			name:    "zero sync interval",
			mutate:  func(c *Config) { c.Storage.SyncInterval = 0 },
			wantErr: "storage.sync-interval must be positive",
		},
		{
			name:    "negative sync interval",
			mutate:  func(c *Config) { c.Storage.SyncInterval = Duration(-time.Millisecond) },
			wantErr: "storage.sync-interval must be positive",
		},
		{
			name:    "negative retention interval",
			mutate:  func(c *Config) { c.Storage.RetentionInterval = Duration(-time.Second) },
			wantErr: "storage.retention-interval must not be negative",
		},
		{
			name:   "zero retention interval disables sweeper",
			mutate: func(c *Config) { c.Storage.RetentionInterval = 0 },
		},
		{
			name:    "zero read limit",
			mutate:  func(c *Config) { c.Subscribe.ReadLimit = 0 },
			wantErr: "subscribe.read-limit must be positive",
		},
		{
			name:    "negative read limit",
			mutate:  func(c *Config) { c.Subscribe.ReadLimit = -1 },
			wantErr: "subscribe.read-limit must be positive",
		},
		{
			name:    "zero read max bytes",
			mutate:  func(c *Config) { c.Subscribe.ReadMaxBytes = 0 },
			wantErr: "subscribe.read-max-bytes must be positive",
		},
		{
			name:    "negative read max bytes",
			mutate:  func(c *Config) { c.Subscribe.ReadMaxBytes = -1 },
			wantErr: "subscribe.read-max-bytes must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() error = nil, want %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestValidateReportsEveryFailure pins that Validate accumulates rather than
// returning on the first problem, so an operator fixes one round of errors.
func TestValidateReportsEveryFailure(t *testing.T) {
	cfg := Config{} // every field zero: the maximum number of violations
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want many errors")
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("Validate() error type = %T, want a joined error", err)
	}
	if got := len(joined.Unwrap()); got != 11 {
		t.Errorf("Validate() reported %d errors, want 11: %v", got, err)
	}
	for _, want := range []string{
		"listen-addr", "data-dir", "max-recv-msg-size", "max-send-msg-size",
		"log-level", "storage.segment-max-bytes", "storage.index-interval-bytes",
		"storage.sync-mode", "storage.sync-interval", "subscribe.read-limit",
		"subscribe.read-max-bytes",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Validate() error is missing %q: %v", want, err)
		}
	}
}

func TestDurationAccessor(t *testing.T) {
	if got := Duration(1500 * time.Millisecond).Duration(); got != 1500*time.Millisecond {
		t.Errorf("Duration() = %v, want 1.5s", got)
	}
	if got := Duration(0).Duration(); got != 0 {
		t.Errorf("Duration() = %v, want 0", got)
	}
}

func TestDurationUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		want    time.Duration
		wantErr bool
	}{
		{name: "milliseconds", yaml: `"100ms"`, want: 100 * time.Millisecond},
		{name: "seconds", yaml: `"10s"`, want: 10 * time.Second},
		{name: "compound", yaml: `"1h30m"`, want: 90 * time.Minute},
		{name: "negative", yaml: `"-5s"`, want: -5 * time.Second},
		{name: "zero", yaml: `"0s"`, want: 0},
		{name: "unquoted scalar", yaml: `250ms`, want: 250 * time.Millisecond},
		{name: "no unit", yaml: `"100"`, wantErr: true},
		{name: "garbage", yaml: `"soon"`, wantErr: true},
		{name: "empty", yaml: `""`, wantErr: true},
		{name: "bare integer is not a duration", yaml: `100`, wantErr: true},
		{name: "sequence is not a string", yaml: `[1, 2]`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d Duration
			err := yaml.Unmarshal([]byte(tt.yaml), &d)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Unmarshal(%s) error = nil, want an error (got %v)", tt.yaml, d.Duration())
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal(%s) error = %v", tt.yaml, err)
			}
			if d.Duration() != tt.want {
				t.Errorf("Unmarshal(%s) = %v, want %v", tt.yaml, d.Duration(), tt.want)
			}
		})
	}
}

func TestDurationUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		want    time.Duration
		wantErr bool
	}{
		{name: "quoted", json: `"250ms"`, want: 250 * time.Millisecond},
		{name: "unquoted", json: `10s`, want: 10 * time.Second},
		{name: "negative", json: `"-1m"`, want: -time.Minute},
		{name: "garbage", json: `"later"`, wantErr: true},
		{name: "number", json: `500`, wantErr: true},
		{name: "empty string", json: `""`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d Duration
			err := d.UnmarshalJSON([]byte(tt.json))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("UnmarshalJSON(%s) error = nil, want an error (got %v)", tt.json, d.Duration())
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalJSON(%s) error = %v", tt.json, err)
			}
			if d.Duration() != tt.want {
				t.Errorf("UnmarshalJSON(%s) = %v, want %v", tt.json, d.Duration(), tt.want)
			}
		})
	}
}

func TestDurationMarshalJSON(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{in: 100 * time.Millisecond, want: `"100ms"`},
		{in: 10 * time.Second, want: `"10s"`},
		{in: 90 * time.Minute, want: `"1h30m0s"`},
		{in: 0, want: `"0s"`},
	}
	for _, tt := range tests {
		got, err := Duration(tt.in).MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON(%v) error = %v", tt.in, err)
		}
		if string(got) != tt.want {
			t.Errorf("MarshalJSON(%v) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

// TestConfigJSONRoundTrip pins that a resolved config survives being marshaled
// and reloaded, which is what the Duration JSON methods exist for.
func TestConfigJSONRoundTrip(t *testing.T) {
	clearEnv(t)
	want, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var got Config
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", data, err)
	}
	if got != want {
		t.Errorf("round-tripped config = %+v, want %+v", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("round-tripped config failed Validate(): %v", err)
	}
}
