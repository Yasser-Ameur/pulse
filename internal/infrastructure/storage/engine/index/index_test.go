package index

import (
	"bytes"
	"errors"
	"testing"

	"github.com/pulse-stream/pulse/internal/domain/offset"
)

func TestAppendAndLookup(t *testing.T) {
	ix := New(100)
	if ix.BaseOffset() != offset.Offset(100) {
		t.Fatalf("BaseOffset() = %v, want 100", ix.BaseOffset())
	}

	if err := ix.Append(0, 0); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := ix.Append(50, 1024); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := ix.Append(120, 4096); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	cases := []struct {
		o      offset.Offset
		pos    int64
		exists bool
	}{
		{100, 0, true}, // at first entry
		{149, 0, true}, // before second entry -> first
		{150, 1024, true},
		{219, 1024, true},
		{220, 4096, true},
		{99, 0, false},     // before segment base
		{1000, 4096, true}, // past last entry -> last
	}
	for _, c := range cases {
		pos, ok := ix.Lookup(c.o)
		if ok != c.exists || (ok && pos != c.pos) {
			t.Errorf("Lookup(%v) = (%d, %v), want (%d, %v)", c.o, pos, ok, c.pos, c.exists)
		}
	}
}

func TestAppendRejectsNonMonotonic(t *testing.T) {
	ix := New(0)
	if err := ix.Append(10, 100); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := ix.Append(9, 200); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Append(regress) error = %v, want ErrCorrupt", err)
	}
	if err := ix.Append(10, 50); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Append(same offset) error = %v, want ErrCorrupt", err)
	}
}

func TestTruncateTo(t *testing.T) {
	ix := New(0)
	for i := uint32(0); i < 10; i++ {
		if err := ix.Append(i*10, i*100); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	ix.TruncateTo(45)
	if ix.Len() != 5 { // entries at rel offsets 0,10,20,30,40 remain
		t.Fatalf("Len() = %d, want 5", ix.Len())
	}
	if pos, ok := ix.Lookup(40); !ok || pos != 400 {
		t.Fatalf("Lookup(40) = (%d, %v), want (400, true)", pos, ok)
	}
	ix.TruncateTo(0)
	if ix.Len() != 0 {
		t.Fatalf("Len() after TruncateTo(0) = %d, want 0", ix.Len())
	}
}

func TestEncodeDecode(t *testing.T) {
	ix := New(5)
	for i := uint32(0); i < 3; i++ {
		if err := ix.Append(i*10, i*7); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	data := ix.Encode()
	if len(data) != 3*EntrySize {
		t.Fatalf("Encode() length = %d, want %d", len(data), 3*EntrySize)
	}
	got, err := Decode(data, 5)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.BaseOffset() != 5 || got.Len() != 3 {
		t.Fatalf("Decode() = base %v, len %d; want 5, 3", got.BaseOffset(), got.Len())
	}
	if !bytes.Equal(got.Encode(), data) {
		t.Fatal("round-trip mismatch")
	}
	if pos, ok := got.Lookup(25); !ok || pos != 14 {
		t.Fatalf("Lookup(25) = (%d, %v), want (14, true)", pos, ok)
	}
}

func TestDecodeRejectsPartialEntry(t *testing.T) {
	if _, err := Decode([]byte{0, 1, 2}, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Decode(partial) error = %v, want ErrCorrupt", err)
	}
	if _, err := Decode([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0}, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Decode(9 bytes) error = %v, want ErrCorrupt", err)
	}
}
