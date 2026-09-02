package topic

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/retention"
)

func TestNewName(t *testing.T) {
	valid := []string{"orders", "orders.v1", "a-b", "A_1", "payments.created"}
	for _, s := range valid {
		if _, err := NewName(s); err != nil {
			t.Errorf("NewName(%q) error = %v, want nil", s, err)
		}
	}

	invalid := []string{"", ".", "..", "a/b", "a b", "a,b", "!", strings.Repeat("a", MaxNameLength+1)}
	for _, s := range invalid {
		if _, err := NewName(s); err != ErrInvalidName {
			t.Errorf("NewName(%q) error = %v, want ErrInvalidName", s, err)
		}
	}

	if _, err := NewName("__internal"); err != ErrReservedName {
		t.Errorf("NewName(%q) error = %v, want ErrReservedName", "__internal", err)
	}

	if got := Name("orders").String(); got != "orders" {
		t.Errorf("Name.String() = %q, want %q", got, "orders")
	}
}

func TestConfigValidate(t *testing.T) {
	t.Run("zero config falls back to defaults", func(t *testing.T) {
		cfg, err := (Config{}).Validate()
		if err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.MaxMessageBytes != DefaultMaxMessageBytes {
			t.Errorf("MaxMessageBytes = %d, want %d", cfg.MaxMessageBytes, DefaultMaxMessageBytes)
		}
		if cfg.Cleanup != CleanupDelete {
			t.Errorf("Cleanup = %q, want %q", cfg.Cleanup, CleanupDelete)
		}
		if cfg.ReplicationFactor != 1 {
			t.Errorf("ReplicationFactor = %d, want 1", cfg.ReplicationFactor)
		}
	})

	t.Run("rejects oversized message limit", func(t *testing.T) {
		if _, err := (Config{MaxMessageBytes: MaxMessageBytes + 1}).Validate(); err == nil {
			t.Fatal("Validate() error = nil, want error")
		}
	})

	t.Run("zero message limit falls back to default", func(t *testing.T) {
		cfg, err := (Config{MaxMessageBytes: 0}).Validate()
		if err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.MaxMessageBytes != DefaultMaxMessageBytes {
			t.Errorf("MaxMessageBytes = %d, want default %d", cfg.MaxMessageBytes, DefaultMaxMessageBytes)
		}
	})

	t.Run("rejects negative message limit", func(t *testing.T) {
		if _, err := (Config{MaxMessageBytes: -1}).Validate(); err == nil {
			t.Fatal("Validate() with -1 error = nil, want error")
		}
	})

	t.Run("rejects negative retention", func(t *testing.T) {
		cfg := Config{MaxMessageBytes: 1024, Retention: retention.Policy{MaxAge: -time.Hour}}
		if _, err := cfg.Validate(); err == nil {
			t.Fatal("Validate() error = nil, want error")
		}
	})

	t.Run("rejects unknown cleanup policy", func(t *testing.T) {
		cfg := Config{MaxMessageBytes: 1024, Cleanup: "shred"}
		if _, err := cfg.Validate(); err == nil {
			t.Fatal("Validate() error = nil, want error")
		}
	})

	t.Run("accepts compact policy", func(t *testing.T) {
		cfg := Config{MaxMessageBytes: 1024, Cleanup: CleanupCompact}
		if _, err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("rejects replication factor other than one", func(t *testing.T) {
		cfg := Config{MaxMessageBytes: 1024, ReplicationFactor: 3}
		if _, err := cfg.Validate(); err == nil {
			t.Fatal("Validate() error = nil, want error")
		}
	})
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Cleanup != CleanupDelete || cfg.ReplicationFactor != 1 {
		t.Fatalf("DefaultConfig() = %+v, want delete policy and RF 1", cfg)
	}
	if _, err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() error = %v", err)
	}
}

func TestErrInvalidPartitionCountWraps(t *testing.T) {
	if !errors.Is(ErrInvalidPartitionCount, ErrInvalidConfig) {
		t.Fatal("ErrInvalidPartitionCount should wrap ErrInvalidConfig")
	}
}
