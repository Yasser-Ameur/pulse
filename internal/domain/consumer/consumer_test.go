package consumer

import (
	"errors"
	"strings"
	"testing"

	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

func TestIDValidate(t *testing.T) {
	valid := []string{"consumer-1", "order_worker", "a"}
	for _, s := range valid {
		if err := ID(s).Validate(); err != nil {
			t.Errorf("ID(%q).Validate() error = %v, want nil", s, err)
		}
	}

	invalid := []string{"", "has/slash", "has\x00nul", strings.Repeat("c", MaxNameLength+1)}
	for _, s := range invalid {
		if err := ID(s).Validate(); err != ErrInvalidName {
			t.Errorf("ID(%q).Validate() error = %v, want ErrInvalidName", s, err)
		}
	}

	if got := ID("c1").String(); got != "c1" {
		t.Errorf("String() = %q, want %q", got, "c1")
	}
}

func TestSubscriptionValidate(t *testing.T) {
	if err := (Subscription{Topic: mustName(t, "orders")}).Validate(); err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
	if err := (Subscription{Topic: ""}).Validate(); !errors.Is(err, ErrInvalidSubscription) {
		t.Errorf("Validate() with empty topic error = %v, want ErrInvalidSubscription", err)
	}
	if err := (Subscription{Consumer: "bad/name", Topic: mustName(t, "orders")}).Validate(); err != ErrInvalidName {
		t.Errorf("Validate() with bad consumer error = %v, want ErrInvalidName", err)
	}
}

func mustName(t *testing.T, s string) topic.Name {
	t.Helper()
	n, err := topic.NewName(s)
	if err != nil {
		t.Fatalf("NewName(%q) error = %v", s, err)
	}
	return n
}
