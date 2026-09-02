// Package codec implements the on-disk batch and record wire format specified
// in docs/Storage.md §3.
//
// A batch is a 52-byte header followed by a length-delimited records section.
// The header carries a CRC-32C over everything from baseOffset to the end of
// the records, so a torn or bit-rotted frame is never trusted.
//
// The record frame itself carries key/value/headers only. Message metadata
// that is not part of the frame (event id, content type, tracing fields, and
// the numeric advisory fields) is persisted as reserved "__pulse.*" system
// headers and restored to Message fields on decode; user headers are passed
// through verbatim.
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/engine/checksum"
)

// Batch format constants.
const (
	// Magic identifies the format family.
	Magic byte = 0x01
	// Version is the batch format version.
	Version byte = 0x01
	// HeaderSize is the byte length of a version 0x01 batch header.
	HeaderSize = 52

	// nullField marks a missing key or value.
	nullField uint32 = 0xFFFFFFFF

	// flagCompressionMask isolates the (currently unimplemented) compression
	// codec bits of a batch's flags field.
	flagCompressionMask uint16 = 0x0007
	// flagSparseOffsets marks a batch whose records are not encoded at
	// contiguous offsets starting from the batch's index (a compacted batch
	// with holes). Set automatically by EncodeBatch.
	flagSparseOffsets uint16 = 0x0008
)

// Reserved header keys carrying message metadata through the record frame.
const (
	HeaderEventID       = message.SystemHeaderPrefix + "event_id"
	HeaderContentType   = message.SystemHeaderPrefix + "content_type"
	HeaderCorrelationID = message.SystemHeaderPrefix + "correlation_id"
	HeaderTraceID       = message.SystemHeaderPrefix + "trace_id"
	HeaderRetryCount    = message.SystemHeaderPrefix + "retry_count"
	HeaderTTLMs         = message.SystemHeaderPrefix + "ttl_ms"
	HeaderPriority      = message.SystemHeaderPrefix + "priority"
	HeaderSchemaVersion = message.SystemHeaderPrefix + "schema_version"
)

// Sentinel errors returned by the codec.
var (
	// ErrBadMagic reports an unknown batch magic number.
	ErrBadMagic = errors.New("bad batch magic")
	// ErrUnsupportedVersion reports an unknown batch format version.
	ErrUnsupportedVersion = errors.New("unsupported batch version")
	// ErrUnsupportedCompression reports a batch carrying a compression codec
	// bit; compression is not implemented yet.
	ErrUnsupportedCompression = errors.New("unsupported compression codec")
	// ErrCrcMismatch reports a batch whose checksum does not cover its bytes.
	ErrCrcMismatch = errors.New("batch crc mismatch")
	// ErrTruncated reports a batch that is shorter than its header or records
	// section requires.
	ErrTruncated = errors.New("truncated batch")
	// ErrInvalidRecordLength reports a record whose declared length exceeds
	// its frame.
	ErrInvalidRecordLength = errors.New("invalid record length")
	// ErrOffsetDeltaOutOfRange reports a record offset outside the batch.
	ErrOffsetDeltaOutOfRange = errors.New("record offset delta out of range")
	// ErrRecordCountMismatch reports a record count that does not match the
	// decoded records.
	ErrRecordCountMismatch = errors.New("record count mismatch")
	// ErrEmptyBatch reports a batch with no records.
	ErrEmptyBatch = message.ErrEmptyBatch
	// ErrInvalidSystemHeader reports an unreadable reserved metadata header.
	ErrInvalidSystemHeader = errors.New("invalid system header")
)

// EncodeBatch serializes b into the on-disk frame. b.BaseOffset and each
// record's Offset must already be assigned.
func EncodeBatch(b *message.RecordBatch) ([]byte, error) {
	if len(b.Records) == 0 {
		return nil, ErrEmptyBatch
	}
	if b.BaseOffset < 0 {
		return nil, fmt.Errorf("%w: negative base offset", offset.ErrInvalid)
	}
	firstMS := b.FirstTimestamp.UnixMilli()
	lastMS := b.LastTimestamp.UnixMilli()

	records := make([]byte, 0, 64*len(b.Records))
	sparse := false
	for i := range b.Records {
		r := &b.Records[i]
		if uint32(r.Offset-b.BaseOffset) != uint32(i) {
			sparse = true
		}
		rec, err := encodeRecord(r, b.BaseOffset, firstMS)
		if err != nil {
			return nil, err
		}
		records = append(records, rec...)
	}

	var flags uint16
	if sparse {
		flags |= flagSparseOffsets
	}

	out := make([]byte, HeaderSize+len(records))
	out[0] = Magic
	out[1] = Version
	binary.BigEndian.PutUint16(out[2:4], flags)
	binary.BigEndian.PutUint64(out[8:16], uint64(b.BaseOffset))
	binary.BigEndian.PutUint32(out[16:20], uint32(len(records)))
	binary.BigEndian.PutUint64(out[20:28], uint64(firstMS))
	binary.BigEndian.PutUint64(out[28:36], uint64(lastMS))
	binary.BigEndian.PutUint32(out[36:40], uint32(len(b.Records)))
	binary.BigEndian.PutUint32(out[40:44], 0) // producerID
	binary.BigEndian.PutUint32(out[44:48], 0) // producerEpoch
	binary.BigEndian.PutUint32(out[48:52], 0) // baseSequence
	copy(out[HeaderSize:], records)

	// CRC covers baseOffset through the end of the records section.
	binary.BigEndian.PutUint32(out[4:8], checksum.Sum(out[8:]))
	return out, nil
}

// DecodeBatch deserializes a complete batch frame (header + records) into a
// RecordBatch. data must be exactly one batch.
func DecodeBatch(data []byte) (*message.RecordBatch, error) {
	if len(data) < HeaderSize {
		return nil, ErrTruncated
	}
	if data[0] != Magic {
		return nil, ErrBadMagic
	}
	if data[1] != Version {
		return nil, ErrUnsupportedVersion
	}
	flags := binary.BigEndian.Uint16(data[2:4])
	if flags&flagCompressionMask != 0 {
		return nil, ErrUnsupportedCompression
	}
	sparse := flags&flagSparseOffsets != 0
	base := offset.Offset(binary.BigEndian.Uint64(data[8:16]))
	batchLen := binary.BigEndian.Uint32(data[16:20])
	firstMS := int64(binary.BigEndian.Uint64(data[20:28]))
	lastMS := int64(binary.BigEndian.Uint64(data[28:36]))
	recordCount := binary.BigEndian.Uint32(data[36:40])

	// The frame length must match the declared records section before the CRC
	// is trusted, so a torn or extended frame reports ErrTruncated rather than
	// a checksum mismatch.
	if int(batchLen) != len(data)-HeaderSize {
		return nil, ErrTruncated
	}
	if binary.BigEndian.Uint32(data[4:8]) != checksum.Sum(data[8:]) {
		return nil, ErrCrcMismatch
	}
	if recordCount == 0 {
		return nil, ErrEmptyBatch
	}
	records := data[HeaderSize : HeaderSize+int(batchLen)]
	cur := &cursor{buf: records}
	decoded := make([]message.Record, 0, recordCount)
	for i := 0; i < int(recordCount); i++ {
		rec, err := decodeRecord(cur, base, firstMS, recordCount, sparse)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, *rec)
	}
	if cur.remaining() != 0 {
		return nil, ErrInvalidRecordLength
	}
	if uint32(len(decoded)) != recordCount {
		return nil, ErrRecordCountMismatch
	}
	return &message.RecordBatch{
		BaseOffset:     base,
		FirstTimestamp: time.UnixMilli(firstMS),
		LastTimestamp:  time.UnixMilli(lastMS),
		Records:        decoded,
	}, nil
}

// encodeRecord serializes a single record. firstMS is the batch first
// timestamp in milliseconds.
func encodeRecord(r *message.Record, base offset.Offset, firstMS int64) ([]byte, error) {
	if r.Offset < base {
		return nil, fmt.Errorf("%w: record offset before base", offset.ErrInvalid)
	}
	delta := uint32(r.Offset - base)
	tsDelta := int32(r.Timestamp.UnixMilli() - firstMS)

	headers := make(map[string]string, len(r.Message.Headers)+8)
	for k, v := range r.Message.Headers {
		headers[k] = v
	}
	systemHeaders(r.Message, headers)
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rec := make([]byte, 0, 32+len(r.Message.Key)+len(r.Message.Payload))
	rec = binary.BigEndian.AppendUint32(rec, 0) // length placeholder
	rec = append(rec, 0)                        // attributes
	rec = binary.BigEndian.AppendUint32(rec, uint32(tsDelta))
	rec = binary.BigEndian.AppendUint32(rec, delta)
	rec = appendBytes(rec, r.Message.Key)
	rec = appendNullableBytes(rec, r.Message.Payload)
	rec = binary.BigEndian.AppendUint32(rec, uint32(len(keys)))
	for _, k := range keys {
		rec = appendBytes(rec, k)
		rec = appendBytes(rec, headers[k])
	}
	binary.BigEndian.PutUint32(rec[0:4], uint32(len(rec)-4))
	return rec, nil
}

// decodeRecord reads one record from cur. firstMS is the batch first
// timestamp; base is the batch base offset. For a non-sparse batch,
// recordCount bounds the record's offset delta so a corrupt frame cannot
// fabricate offsets outside the batch; a sparse (compacted) batch instead
// accepts any delta within a generous sanity bound, since survivors keep
// their original, non-contiguous offsets.
func decodeRecord(cur *cursor, base offset.Offset, firstMS int64, recordCount uint32, sparse bool) (*message.Record, error) {
	length, err := cur.readUint32()
	if err != nil {
		return nil, err
	}
	body, err := cur.readBytes(int(length))
	if err != nil {
		return nil, ErrInvalidRecordLength
	}
	c := &cursor{buf: body}
	if _, err := c.readByte(); err != nil { // attributes (reserved)
		return nil, ErrInvalidRecordLength
	}
	tsDelta, err := c.readInt32()
	if err != nil {
		return nil, ErrInvalidRecordLength
	}
	offDelta, err := c.readUint32()
	if err != nil {
		return nil, ErrInvalidRecordLength
	}
	if sparse {
		if offDelta >= 1<<31 {
			return nil, ErrOffsetDeltaOutOfRange
		}
	} else if offDelta >= recordCount {
		return nil, ErrOffsetDeltaOutOfRange
	}
	key, err := c.readNullableString()
	if err != nil {
		return nil, ErrInvalidRecordLength
	}
	value, err := c.readNullableBytes()
	if err != nil {
		return nil, ErrInvalidRecordLength
	}
	headerCount, err := c.readUint32()
	if err != nil {
		return nil, ErrInvalidRecordLength
	}
	headers := make(message.Headers, headerCount)
	for i := 0; i < int(headerCount); i++ {
		k, err := c.readNullableString()
		if err != nil {
			return nil, ErrInvalidRecordLength
		}
		v, err := c.readNullableString()
		if err != nil {
			return nil, ErrInvalidRecordLength
		}
		headers[k] = v
	}
	if c.remaining() != 0 {
		return nil, ErrInvalidRecordLength
	}
	m, err := messageFromHeaders(headers, key, value)
	if err != nil {
		return nil, err
	}
	return &message.Record{
		Offset:    base + offset.Offset(offDelta),
		Timestamp: time.UnixMilli(firstMS + int64(tsDelta)),
		Message:   m,
	}, nil
}

// systemHeaders copies the message's metadata fields into headers using the
// reserved key namespace.
func systemHeaders(m message.Message, headers map[string]string) {
	if !m.EventID.Zero() {
		headers[HeaderEventID] = m.EventID.String()
	}
	if m.ContentType != "" {
		headers[HeaderContentType] = m.ContentType
	}
	if m.CorrelationID != "" {
		headers[HeaderCorrelationID] = m.CorrelationID
	}
	if m.TraceID != "" {
		headers[HeaderTraceID] = m.TraceID
	}
	if m.RetryCount != 0 {
		headers[HeaderRetryCount] = strconv.FormatInt(int64(m.RetryCount), 10)
	}
	if m.TTL != 0 {
		headers[HeaderTTLMs] = strconv.FormatInt(m.TTL.Milliseconds(), 10)
	}
	if m.Priority != 0 {
		headers[HeaderPriority] = strconv.FormatInt(int64(m.Priority), 10)
	}
	if m.SchemaVersion != 0 {
		headers[HeaderSchemaVersion] = strconv.FormatInt(int64(m.SchemaVersion), 10)
	}
}

// messageFromHeaders rebuilds a Message from a record frame, stripping the
// reserved system headers back into fields. User headers are passed through
// verbatim.
func messageFromHeaders(headers message.Headers, key string, value []byte) (message.Message, error) {
	m := message.Message{Key: key, Payload: value}
	for k, v := range headers {
		if len(k) >= len(message.SystemHeaderPrefix) && k[:len(message.SystemHeaderPrefix)] == message.SystemHeaderPrefix {
			if err := applySystemHeader(&m, k, v); err != nil {
				return message.Message{}, err
			}
			continue
		}
		if m.Headers == nil {
			m.Headers = make(message.Headers, len(headers))
		}
		m.Headers[k] = v
	}
	return m, nil
}

// applySystemHeader maps one reserved header back to its Message field.
func applySystemHeader(m *message.Message, key, value string) error {
	switch key {
	case HeaderEventID:
		id, err := message.ParseEventID(value)
		if err != nil {
			return err
		}
		m.EventID = id
	case HeaderContentType:
		m.ContentType = value
	case HeaderCorrelationID:
		m.CorrelationID = value
	case HeaderTraceID:
		m.TraceID = value
	case HeaderRetryCount:
		n, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return err
		}
		m.RetryCount = int32(n)
	case HeaderTTLMs:
		ms, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		m.TTL = time.Duration(ms) * time.Millisecond
	case HeaderPriority:
		n, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return err
		}
		m.Priority = int32(n)
	case HeaderSchemaVersion:
		n, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return err
		}
		m.SchemaVersion = int32(n)
	default:
		return fmt.Errorf("%w: %s", ErrInvalidSystemHeader, key)
	}
	return nil
}

// appendBytes writes a length-prefixed non-null string.
func appendBytes(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

// appendNullableBytes writes a length-prefixed value, encoding a nil slice as
// nullField (a tombstone marker) and any non-nil slice, including an empty
// one, as its literal length. This is what lets a record's payload
// distinguish "no value" from "zero-length value".
func appendNullableBytes(buf []byte, b []byte) []byte {
	if b == nil {
		return binary.BigEndian.AppendUint32(buf, nullField)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...)
}

// cursor is a bounds-checked big-endian reader.
type cursor struct {
	buf []byte
	pos int
}

func (c *cursor) remaining() int { return len(c.buf) - c.pos }

func (c *cursor) readByte() (byte, error) {
	if c.remaining() < 1 {
		return 0, ErrTruncated
	}
	b := c.buf[c.pos]
	c.pos++
	return b, nil
}

func (c *cursor) readUint32() (uint32, error) {
	if c.remaining() < 4 {
		return 0, ErrTruncated
	}
	v := binary.BigEndian.Uint32(c.buf[c.pos : c.pos+4])
	c.pos += 4
	return v, nil
}

func (c *cursor) readInt32() (int32, error) {
	v, err := c.readUint32()
	return int32(v), err
}

func (c *cursor) readBytes(n int) ([]byte, error) {
	if n < 0 || c.remaining() < n {
		return nil, ErrTruncated
	}
	out := c.buf[c.pos : c.pos+n]
	c.pos += n
	return out, nil
}

func (c *cursor) readNullableBytes() ([]byte, error) {
	n, err := c.readUint32()
	if err != nil {
		return nil, err
	}
	if n == nullField {
		return nil, nil
	}
	return c.readBytes(int(n))
}

func (c *cursor) readNullableString() (string, error) {
	b, err := c.readNullableBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}
