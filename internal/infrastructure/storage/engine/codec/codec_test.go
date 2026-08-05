package codec

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/message"
)

func sampleBatch() *message.RecordBatch {
	now := time.Unix(1700000000, 0).UTC()
	id, _ := message.ParseEventID("01ARZ3NDEKTSV4RRFFQ69G5FAV") // fixed: keep encoding deterministic
	return &message.RecordBatch{
		BaseOffset:     10,
		FirstTimestamp: now,
		LastTimestamp:  now.Add(time.Millisecond),
		Records: []message.Record{
			{
				Offset:    10,
				Timestamp: now,
				Message: message.Message{
					EventID:       id,
					Key:           "orders",
					Payload:       []byte("payload-one"),
					ContentType:   "application/json",
					CorrelationID: "corr-1",
					TraceID:       "trace-1",
					RetryCount:    2,
					TTL:           time.Minute,
					Priority:      3,
					SchemaVersion: 4,
					Headers:       message.Headers{"region": "us", "env": "prod"},
				},
			},
			{
				Offset:    11,
				Timestamp: now.Add(time.Millisecond),
				Message:   message.Message{Key: "b", Payload: []byte("two")},
			},
			{
				Offset:    12,
				Timestamp: now.Add(time.Millisecond),
				Message:   message.Message{Payload: []byte{}},
			},
		},
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	b := sampleBatch()
	enc, err := EncodeBatch(b)
	if err != nil {
		t.Fatalf("EncodeBatch() error = %v", err)
	}
	if len(enc) != HeaderSize+recordRegionLen(b) {
		t.Fatalf("encoded length = %d, want header+records", len(enc))
	}

	got, err := DecodeBatch(enc)
	if err != nil {
		t.Fatalf("DecodeBatch() error = %v", err)
	}
	if got.BaseOffset != b.BaseOffset {
		t.Errorf("BaseOffset = %v, want %v", got.BaseOffset, b.BaseOffset)
	}
	if !got.FirstTimestamp.Equal(b.FirstTimestamp) || !got.LastTimestamp.Equal(b.LastTimestamp) {
		t.Errorf("timestamps not preserved: %v..%v", got.FirstTimestamp, got.LastTimestamp)
	}
	if len(got.Records) != 3 {
		t.Fatalf("records = %d, want 3", len(got.Records))
	}

	for i, want := range b.Records {
		r := got.Records[i]
		if r.Offset != want.Offset {
			t.Errorf("record %d offset = %v, want %v", i, r.Offset, want.Offset)
		}
		if r.Message.EventID != want.Message.EventID {
			t.Errorf("record %d event id = %q, want %q", i, r.Message.EventID, want.Message.EventID)
		}
		if r.Message.Key != want.Message.Key {
			t.Errorf("record %d key = %q, want %q", i, r.Message.Key, want.Message.Key)
		}
		if string(r.Message.Payload) != string(want.Message.Payload) {
			t.Errorf("record %d payload = %q, want %q", i, r.Message.Payload, want.Message.Payload)
		}
		if r.Message.ContentType != want.Message.ContentType ||
			r.Message.CorrelationID != want.Message.CorrelationID ||
			r.Message.TraceID != want.Message.TraceID {
			t.Errorf("record %d metadata not preserved", i)
		}
		if r.Message.RetryCount != want.Message.RetryCount ||
			r.Message.TTL != want.Message.TTL ||
			r.Message.Priority != want.Message.Priority ||
			r.Message.SchemaVersion != want.Message.SchemaVersion {
			t.Errorf("record %d numeric fields not preserved", i)
		}
		for k, v := range want.Message.Headers {
			if r.Message.Headers[k] != v {
				t.Errorf("record %d header %q = %q, want %q", i, k, r.Message.Headers[k], v)
			}
		}
		if len(r.Message.Headers) != len(want.Message.Headers) {
			t.Errorf("record %d header count = %d, want %d", i, len(r.Message.Headers), len(want.Message.Headers))
		}
		if r.Timestamp.UnixMilli() != want.Timestamp.UnixMilli() {
			t.Errorf("record %d timestamp = %v, want %v", i, r.Timestamp, want.Timestamp)
		}
	}
}

func TestEncodeDeterministic(t *testing.T) {
	a, _ := EncodeBatch(sampleBatch())
	b, _ := EncodeBatch(sampleBatch())
	if !bytes.Equal(a, b) {
		t.Fatal("encode is not deterministic for equal batches")
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	enc, err := EncodeBatch(sampleBatch())
	if err != nil {
		t.Fatalf("EncodeBatch() error = %v", err)
	}

	t.Run("truncated frame", func(t *testing.T) {
		if _, err := DecodeBatch(enc[:HeaderSize-1]); !errors.Is(err, ErrTruncated) {
			t.Fatalf("DecodeBatch(short) error = %v, want ErrTruncated", err)
		}
	})

	t.Run("bad magic", func(t *testing.T) {
		bad := bytes.Clone(enc)
		bad[0] = 0xFF
		if _, err := DecodeBatch(bad); !errors.Is(err, ErrBadMagic) {
			t.Fatalf("DecodeBatch(magic) error = %v, want ErrBadMagic", err)
		}
	})

	t.Run("bad version", func(t *testing.T) {
		bad := bytes.Clone(enc)
		bad[1] = 0x02
		if _, err := DecodeBatch(bad); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("DecodeBatch(version) error = %v, want ErrUnsupportedVersion", err)
		}
	})

	t.Run("compression bit", func(t *testing.T) {
		bad := bytes.Clone(enc)
		bad[3] |= 0x01 // flags low bit = compression codec
		if _, err := DecodeBatch(bad); !errors.Is(err, ErrUnsupportedCompression) {
			t.Fatalf("DecodeBatch(flags) error = %v, want ErrUnsupportedCompression", err)
		}
	})

	t.Run("crc mismatch on payload", func(t *testing.T) {
		bad := bytes.Clone(enc)
		bad[HeaderSize+4] ^= 0x01
		if _, err := DecodeBatch(bad); !errors.Is(err, ErrCrcMismatch) {
			t.Fatalf("DecodeBatch(crc) error = %v, want ErrCrcMismatch", err)
		}
	})

	t.Run("trailing garbage", func(t *testing.T) {
		bad := append(bytes.Clone(enc), 0x00)
		if _, err := DecodeBatch(bad); !errors.Is(err, ErrTruncated) {
			t.Fatalf("DecodeBatch(trailing) error = %v, want ErrTruncated", err)
		}
	})
}

func TestDecodeRejectsOutOfRangeOffsetDelta(t *testing.T) {
	// A record whose offset delta is outside the batch's record count must be
	// rejected even though the CRC is valid.
	b := &message.RecordBatch{
		BaseOffset:     100,
		FirstTimestamp: time.Now(),
		LastTimestamp:  time.Now(),
		Records: []message.Record{{
			Offset:  105, // delta 5 with recordCount 1
			Message: message.Message{Payload: []byte("x")},
		}},
	}
	enc, err := EncodeBatch(b)
	if err != nil {
		t.Fatalf("EncodeBatch() error = %v", err)
	}
	if _, err := DecodeBatch(enc); !errors.Is(err, ErrOffsetDeltaOutOfRange) {
		t.Fatalf("DecodeBatch() error = %v, want ErrOffsetDeltaOutOfRange", err)
	}
}

func TestEmptyBatchRejected(t *testing.T) {
	if _, err := EncodeBatch(&message.RecordBatch{}); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("EncodeBatch(empty) error = %v, want ErrEmptyBatch", err)
	}
}

func recordRegionLen(b *message.RecordBatch) int {
	var total int
	for i := range b.Records {
		rec, err := encodeRecord(&b.Records[i], b.BaseOffset, b.FirstTimestamp.UnixMilli())
		if err != nil {
			return -1
		}
		total += len(rec)
	}
	return total
}
