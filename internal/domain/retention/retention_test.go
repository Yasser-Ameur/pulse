package retention

import (
	"errors"
	"testing"
	"time"
)

func TestPolicyValidate(t *testing.T) {
	if err := (Policy{}).Validate(); err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
	if err := (Policy{MaxAge: 24 * time.Hour, MaxBytes: 1 << 30}).Validate(); err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
	if err := (Policy{MaxAge: -time.Hour}).Validate(); !errors.Is(err, ErrNegativeMaxAge) {
		t.Errorf("Validate() negative max age error = %v, want ErrNegativeMaxAge", err)
	}
	if err := (Policy{MaxBytes: -1}).Validate(); !errors.Is(err, ErrNegativeMaxBytes) {
		t.Errorf("Validate() negative max bytes error = %v, want ErrNegativeMaxBytes", err)
	}
}
