package snapshot

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/checksum"
)

func TestWriteLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := State{
		LEO:        offset.Offset(42),
		ActiveBase: offset.Offset(10),
		ActiveSize: 1024,
		ActiveNext: offset.Offset(42),
	}
	if err := Write(dir, st); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	got, present, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !present {
		t.Fatal("Load() present = false, want true")
	}
	if got != st {
		t.Fatalf("Load() = %+v, want %+v", got, st)
	}
}

func TestLoadAbsent(t *testing.T) {
	dir := t.TempDir()
	if _, present, err := Load(dir); err != nil || present {
		t.Fatalf("Load() = (%v, %v), want (nil, false)", present, err)
	}
}

func TestLoadRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	st := State{LEO: 1, ActiveBase: 0, ActiveSize: 100, ActiveNext: 1}
	if err := Write(dir, st); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	path := filepath.Join(dir, "snapshot")

	// Flip a payload byte: the CRC must reject it.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	data[20] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}
	if _, present, err := Load(dir); !present || err == nil {
		t.Fatalf("Load() = (%v, %v), want (present=true, ErrCorrupt)", present, err)
	}
}

func TestLoadRejectsInconsistentState(t *testing.T) {
	dir := t.TempDir()
	// ActiveNext must equal LEO; write an invalid one by hand.
	data := make([]byte, FileSize)
	data[0] = Magic
	data[1] = Version
	data[8] = 0
	data[16] = 0
	data[32] = 1 // ActiveNext = 1 != LEO = 0
	binary.BigEndian.PutUint32(data[4:8], checksum.Sum(data[8:]))
	if err := os.WriteFile(filepath.Join(dir, "snapshot"), data, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if _, present, err := Load(dir); !present || err == nil {
		t.Fatalf("Load() = (%v, %v), want (present=true, ErrCorrupt)", present, err)
	}
}

func TestWriteRejectsInvalidState(t *testing.T) {
	dir := t.TempDir()
	if err := Write(dir, State{LEO: -1, ActiveBase: 0, ActiveNext: -1}); err == nil {
		t.Fatal("Write() error = nil, want error for invalid state")
	}
	if err := Write(dir, State{LEO: 1, ActiveBase: 2, ActiveNext: 1}); err == nil {
		t.Fatal("Write() error = nil, want error for ActiveBase > LEO")
	}
}
