package services

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Yasser-Ameur/pulse/internal/domain/message"
	"github.com/Yasser-Ameur/pulse/internal/domain/offset"
	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
)

func newTestPublisher(t *testing.T) (*Publisher, *LogRegistry, topic.Topic, *fakeLog) {
	t.Helper()
	r := NewLogRegistry()
	name := mustName(t, "orders")
	tpc := testTopic(name, 1)
	r.RegisterTopic(tpc)
	lg := newFakeLog()
	r.RegisterLog(name, partition.ID(0), lg)
	var publishedRecords, publishedBytes atomic.Int64
	p := NewPublisher(r, &fakeClock{now: timeNow()}, &fakeLogger{}, fakeMetrics{}, &publishedRecords, &publishedBytes)
	return p, r, tpc, lg
}

func TestPublisherAssignsOffsetsAndEventIDs(t *testing.T) {
	p, _, tpc, lg := newTestPublisher(t)
	msgs := []message.Message{
		{Payload: []byte("one")},
		{Payload: []byte("two")},
		{Payload: []byte("three")},
	}
	offsets, err := p.Publish(context.Background(), tpc, partition.ID(0), msgs)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(offsets) != 3 {
		t.Fatalf("len(offsets) = %d, want 3", len(offsets))
	}
	for i, o := range offsets {
		if o != offset.Offset(i) {
			t.Errorf("offsets[%d] = %v, want %v", i, o, i)
		}
	}
	if lg.NextOffset() != offset.Offset(3) {
		t.Errorf("NextOffset() = %v, want 3", lg.NextOffset())
	}

	records := lg.records
	if len(records) != 3 {
		t.Fatalf("stored records = %d, want 3", len(records))
	}
	for i, r := range records {
		if r.Message.EventID.Zero() {
			t.Errorf("record %d has a zero event id", i)
		}
		if r.Offset != offset.Offset(i) {
			t.Errorf("record %d offset = %v, want %v", i, r.Offset, i)
		}
		if r.Timestamp.IsZero() {
			t.Errorf("record %d has a zero timestamp", i)
		}
	}
}

func TestPublisherPreservesClientEventID(t *testing.T) {
	p, _, tpc, _ := newTestPublisher(t)
	id := message.NewEventID(timeNow())
	_, err := p.Publish(context.Background(), tpc, partition.ID(0), []message.Message{{Payload: []byte("x"), EventID: id}})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
}

func TestPublisherRejectsEmptyBatch(t *testing.T) {
	p, _, tpc, _ := newTestPublisher(t)
	if _, err := p.Publish(context.Background(), tpc, partition.ID(0), nil); !errors.Is(err, message.ErrEmptyBatch) {
		t.Fatalf("Publish() error = %v, want ErrEmptyBatch", err)
	}
}

func TestPublisherRejectsOversizedBatch(t *testing.T) {
	p, _, tpc, _ := newTestPublisher(t)
	msgs := make([]message.Message, message.MaxBatchRecords+1)
	for i := range msgs {
		msgs[i] = message.Message{Payload: []byte("x")}
	}
	if _, err := p.Publish(context.Background(), tpc, partition.ID(0), msgs); !errors.Is(err, message.ErrBatchTooLarge) {
		t.Fatalf("Publish() error = %v, want ErrBatchTooLarge", err)
	}
}

func TestPublisherValidatesMessages(t *testing.T) {
	p, _, tpc, _ := newTestPublisher(t)
	// Topic default max message bytes is 1 MiB; oversized payload must fail
	// before any append.
	_, err := p.Publish(context.Background(), tpc, partition.ID(0), []message.Message{
		{Payload: []byte("valid")},
		{Payload: make([]byte, topic.DefaultMaxMessageBytes+1)},
	})
	if !errors.Is(err, message.ErrPayloadTooLarge) {
		t.Fatalf("Publish() error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestPublisherMissingPartition(t *testing.T) {
	p, _, tpc, _ := newTestPublisher(t)
	if _, err := p.Publish(context.Background(), tpc, partition.ID(7), []message.Message{{Payload: []byte("x")}}); !errors.Is(err, partition.ErrNotFound) {
		t.Fatalf("Publish() error = %v, want partition.ErrNotFound", err)
	}
}

func TestPublisherAppendFailure(t *testing.T) {
	p, _, tpc, lg := newTestPublisher(t)
	lg.appendErr = errors.New("disk full")
	if _, err := p.Publish(context.Background(), tpc, partition.ID(0), []message.Message{{Payload: []byte("x")}}); err == nil {
		t.Fatal("Publish() error = nil, want append failure")
	}
}
