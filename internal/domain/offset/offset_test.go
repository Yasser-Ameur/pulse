package offset

import (
	"errors"
	"testing"
)

func TestOffset(t *testing.T) {
	if !Offset(0).Valid() {
		t.Fatal("Offset(0) should be valid")
	}
	if Offset(42).Valid() != true {
		t.Fatal("Offset(42) should be valid")
	}
	if Invalid.Valid() {
		t.Fatal("Invalid offset should not be valid")
	}

	if got := Offset(7).String(); got != "7" {
		t.Errorf("String() = %q, want %q", got, "7")
	}
	if got := Offset(7).Int64(); got != 7 {
		t.Errorf("Int64() = %d, want 7", got)
	}
}

func TestErrInvalidWrapsOutOfRange(t *testing.T) {
	if !errors.Is(ErrInvalid, ErrOutOfRange) {
		t.Fatal("ErrInvalid should wrap ErrOutOfRange")
	}
}
