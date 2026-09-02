package message

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
)

func TestMessageValidate(t *testing.T) {
	base := Message{Payload: []byte("ok")}

	t.Run("valid message passes", func(t *testing.T) {
		m := base
		if err := m.Validate(1024); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("payload over topic limit", func(t *testing.T) {
		m := Message{Payload: make([]byte, 9)}
		if err := m.Validate(8); err != ErrPayloadTooLarge {
			t.Fatalf("Validate() error = %v, want ErrPayloadTooLarge", err)
		}
	})

	t.Run("payload exactly at limit passes", func(t *testing.T) {
		m := Message{Payload: make([]byte, 8)}
		if err := m.Validate(8); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("key too long", func(t *testing.T) {
		m := Message{Payload: []byte("x"), Key: strings.Repeat("k", MaxKeyBytes+1)}
		if err := m.Validate(1024); err != ErrKeyTooLong {
			t.Fatalf("Validate() error = %v, want ErrKeyTooLong", err)
		}
	})

	t.Run("too many headers", func(t *testing.T) {
		h := make(Headers, MaxHeaders+1)
		for i := 0; i <= MaxHeaders; i++ {
			h["key-"+strings.Repeat("x", 1)+strconv.Itoa(i)] = "v"
		}
		m := Message{Payload: []byte("x"), Headers: h}
		if err := m.Validate(1024); err != ErrHeadersTooMany {
			t.Fatalf("Validate() error = %v, want ErrHeadersTooMany", err)
		}
	})

	t.Run("header key too long", func(t *testing.T) {
		m := Message{
			Payload: []byte("x"),
			Headers: Headers{strings.Repeat("k", MaxHeaderKeyBytes+1): "v"},
		}
		if err := m.Validate(1024); err != ErrHeaderKeyTooLong {
			t.Fatalf("Validate() error = %v, want ErrHeaderKeyTooLong", err)
		}
	})

	t.Run("header value too long", func(t *testing.T) {
		m := Message{
			Payload: []byte("x"),
			Headers: Headers{"k": strings.Repeat("v", MaxHeaderValueBytes+1)},
		}
		if err := m.Validate(1024); err != ErrHeaderValueTooLong {
			t.Fatalf("Validate() error = %v, want ErrHeaderValueTooLong", err)
		}
	})
}

func TestEventID(t *testing.T) {
	id := NewEventID(time.Unix(1700000000, 0).UTC())
	if id.Zero() {
		t.Fatal("NewEventID() returned the zero value")
	}
	s := id.String()
	if len(s) != 26 {
		t.Fatalf("EventID.String() length = %d, want 26", len(s))
	}
	if s != strings.ToUpper(s) {
		t.Fatalf("EventID.String() = %q, want uppercase Crockford base32", s)
	}

	t.Run("parse round-trips", func(t *testing.T) {
		got, err := ParseEventID(s)
		if err != nil {
			t.Fatalf("ParseEventID() error = %v", err)
		}
		if got != id {
			t.Fatalf("ParseEventID() = %v, want %v", got, id)
		}
	})

	t.Run("parse tolerates lowercase", func(t *testing.T) {
		got, err := ParseEventID(strings.ToLower(s))
		if err != nil {
			t.Fatalf("ParseEventID(lower) error = %v", err)
		}
		if got != id {
			t.Fatalf("ParseEventID(lower) = %v, want %v", got, id)
		}
	})

	t.Run("parse rejects garbage", func(t *testing.T) {
		for _, bad := range []string{"", "not-a-ulid", "!!!!!", strings.Repeat("A", 25)} {
			if _, err := ParseEventID(bad); err != ErrInvalidEventID {
				t.Fatalf("ParseEventID(%q) error = %v, want ErrInvalidEventID", bad, err)
			}
		}
	})
}

func TestRecordBatchOffsets(t *testing.T) {
	b := RecordBatch{
		BaseOffset: offset.Invalid,
		Records:    []Record{{}, {}},
	}
	if b.BaseOffset != offset.Invalid {
		t.Fatalf("BaseOffset = %v, want Invalid", b.BaseOffset)
	}
}
