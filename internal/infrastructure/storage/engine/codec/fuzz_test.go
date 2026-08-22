package codec

import (
	"bytes"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
)

// maxFuzzMillis keeps generated timestamps inside the range time.UnixMilli
// round-trips exactly, so the fuzzer explores encoding rather than calendar
// arithmetic.
const maxFuzzMillis = 1 << 50

// FuzzDecodeBatch feeds arbitrary bytes to the batch decoder. A batch frame is
// attacker-reachable through the log files, so decoding must never panic, and
// any frame the decoder accepts must be a fixed point: re-encoding it and
// decoding again yields the same batch.
func FuzzDecodeBatch(f *testing.F) {
	valid, err := EncodeBatch(sampleBatch())
	if err != nil {
		f.Fatalf("EncodeBatch() error = %v", err)
	}
	f.Add(valid)
	f.Add([]byte(nil))
	f.Add(make([]byte, HeaderSize))      // zeroed header
	f.Add(valid[:HeaderSize])            // header without its records
	f.Add(valid[:len(valid)-1])          // torn tail
	f.Add(append(valid[:0:0], valid...)) // independent copy for mutation

	corrupt := append([]byte(nil), valid...)
	corrupt[HeaderSize] ^= 0xFF // payload bit flip: CRC must catch it
	f.Add(corrupt)

	badMagic := append([]byte(nil), valid...)
	badMagic[0] = 0x00
	f.Add(badMagic)

	f.Fuzz(func(t *testing.T, data []byte) {
		batch, err := DecodeBatch(data)
		if err != nil {
			if batch != nil {
				t.Fatalf("DecodeBatch() returned a batch alongside error %v", err)
			}
			return
		}
		if len(batch.Records) == 0 {
			t.Fatal("DecodeBatch() accepted a batch with no records")
		}
		// Every record must sit inside the batch's own offset span; a corrupt
		// frame must not fabricate offsets belonging to another batch.
		end := batch.BaseOffset + offset.Offset(len(batch.Records))
		for i, r := range batch.Records {
			if r.Offset < batch.BaseOffset || r.Offset >= end {
				t.Fatalf("record %d offset = %v, want within [%v, %v)", i, r.Offset, batch.BaseOffset, end)
			}
		}

		reencoded, err := EncodeBatch(batch)
		if err != nil {
			t.Fatalf("EncodeBatch(decoded batch) error = %v", err)
		}
		again, err := DecodeBatch(reencoded)
		if err != nil {
			t.Fatalf("DecodeBatch(re-encoded batch) error = %v", err)
		}
		assertBatchEqual(t, again, batch)
	})
}

// FuzzEncodeDecodeRoundTrip generates messages the domain accepts and asserts
// the codec preserves them exactly. Field metadata travels as reserved system
// headers, so this is where lossy or colliding header mapping shows up.
func FuzzEncodeDecodeRoundTrip(f *testing.F) {
	f.Add(int64(0), int64(0), int32(0), "", []byte(nil), "", "", "", int32(0), int32(0), int32(0), false)
	f.Add(int64(10), int64(1700000000000), int32(1), "orders", []byte("payload"),
		"region", "us-east", "application/json", int32(3), int32(60000), int32(7), true)
	f.Add(int64(1<<40), int64(-1), int32(-1000), "\x00\xff", []byte{0x00, 0x01, 0xff},
		"", "", "text/plain", int32(-1), int32(-1), int32(-1), false)

	f.Fuzz(func(t *testing.T,
		baseOffset, firstMS int64, tsDelta int32,
		key string, payload []byte,
		headerKey, headerValue, contentType string,
		retryCount, ttlMS, priority int32,
		withEventID bool,
	) {
		if baseOffset < 0 || baseOffset > maxFuzzMillis {
			t.Skip("negative or absurd base offsets are rejected before encoding")
		}
		if firstMS < -maxFuzzMillis || firstMS > maxFuzzMillis {
			t.Skip("timestamp outside the range UnixMilli round-trips exactly")
		}

		msg := message.Message{
			Key:         key,
			Payload:     payload,
			ContentType: contentType,
			RetryCount:  retryCount,
			TTL:         time.Duration(ttlMS) * time.Millisecond,
			Priority:    priority,
		}
		if headerKey != "" || headerValue != "" {
			msg.Headers = message.Headers{headerKey: headerValue}
		}
		if withEventID {
			id, err := message.ParseEventID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
			if err != nil {
				t.Fatalf("ParseEventID() error = %v", err)
			}
			msg.EventID = id
		}
		if err := msg.Validate(1 << 20); err != nil {
			t.Skip("message the domain would reject before it reaches the codec")
		}

		base := offset.Offset(baseOffset)
		first := time.UnixMilli(firstMS).UTC()
		second := time.UnixMilli(firstMS + int64(tsDelta)).UTC()
		batch := &message.RecordBatch{
			BaseOffset:     base,
			FirstTimestamp: first,
			LastTimestamp:  second,
			Records: []message.Record{
				{Offset: base, Timestamp: first, Message: msg},
				{Offset: base + 1, Timestamp: second, Message: message.Message{Payload: []byte("tail")}},
			},
		}

		data, err := EncodeBatch(batch)
		if err != nil {
			t.Fatalf("EncodeBatch() error = %v", err)
		}
		decoded, err := DecodeBatch(data)
		if err != nil {
			t.Fatalf("DecodeBatch() error = %v", err)
		}
		assertBatchEqual(t, decoded, batch)
	})
}

func assertBatchEqual(t *testing.T, got, want *message.RecordBatch) {
	t.Helper()
	if got.BaseOffset != want.BaseOffset {
		t.Fatalf("BaseOffset = %v, want %v", got.BaseOffset, want.BaseOffset)
	}
	if !got.FirstTimestamp.Equal(want.FirstTimestamp) {
		t.Fatalf("FirstTimestamp = %v, want %v", got.FirstTimestamp, want.FirstTimestamp)
	}
	if !got.LastTimestamp.Equal(want.LastTimestamp) {
		t.Fatalf("LastTimestamp = %v, want %v", got.LastTimestamp, want.LastTimestamp)
	}
	if len(got.Records) != len(want.Records) {
		t.Fatalf("records = %d, want %d", len(got.Records), len(want.Records))
	}
	for i := range want.Records {
		assertRecordEqual(t, i, got.Records[i], want.Records[i])
	}
}

func assertRecordEqual(t *testing.T, i int, got, want message.Record) {
	t.Helper()
	if got.Offset != want.Offset {
		t.Fatalf("record %d Offset = %v, want %v", i, got.Offset, want.Offset)
	}
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("record %d Timestamp = %v, want %v", i, got.Timestamp, want.Timestamp)
	}
	g, w := got.Message, want.Message
	if g.Key != w.Key {
		t.Fatalf("record %d Key = %q, want %q", i, g.Key, w.Key)
	}
	if !bytes.Equal(g.Payload, w.Payload) {
		t.Fatalf("record %d Payload = %q, want %q", i, g.Payload, w.Payload)
	}
	if g.EventID != w.EventID {
		t.Fatalf("record %d EventID = %v, want %v", i, g.EventID, w.EventID)
	}
	if g.ContentType != w.ContentType || g.CorrelationID != w.CorrelationID || g.TraceID != w.TraceID {
		t.Fatalf("record %d metadata = %q/%q/%q, want %q/%q/%q", i,
			g.ContentType, g.CorrelationID, g.TraceID, w.ContentType, w.CorrelationID, w.TraceID)
	}
	if g.RetryCount != w.RetryCount || g.Priority != w.Priority || g.SchemaVersion != w.SchemaVersion {
		t.Fatalf("record %d counters = %d/%d/%d, want %d/%d/%d", i,
			g.RetryCount, g.Priority, g.SchemaVersion, w.RetryCount, w.Priority, w.SchemaVersion)
	}
	if g.TTL != w.TTL {
		t.Fatalf("record %d TTL = %v, want %v", i, g.TTL, w.TTL)
	}
	// An absent header map and an empty one carry the same information.
	if len(g.Headers) != len(w.Headers) {
		t.Fatalf("record %d headers = %v, want %v", i, g.Headers, w.Headers)
	}
	for k, v := range w.Headers {
		if g.Headers[k] != v {
			t.Fatalf("record %d header %q = %q, want %q", i, k, g.Headers[k], v)
		}
	}
}
