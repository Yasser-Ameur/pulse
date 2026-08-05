package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

func timeNow() time.Time { return time.Unix(1700000000, 0).UTC() }

func newTestSubscriber(t *testing.T) (*Subscriber, *LogRegistry, *fakeStore, *fakeLog) {
	t.Helper()
	r := NewLogRegistry()
	name := mustName(t, "orders")
	r.RegisterTopic(testTopic(name, 1))
	lg := newFakeLog()
	r.RegisterLog(name, partition.ID(0), lg)
	store := newFakeStore()
	s := NewSubscriber(r, store, SubscriberOptions{ReadLimit: 10, ReadMaxBytes: 1 << 20}, &fakeLogger{}, fakeMetrics{})
	return s, r, store, lg
}

func TestSubscriberReplaysFromStart(t *testing.T) {
	s, _, _, lg := newTestSubscriber(t)
	appendRecords(t, lg, 3)

	var got []message.Record
	err := s.Subscribe(context.Background(), consumer.Subscription{Topic: mustName(t, "orders"), Partition: 0}, func(recs []message.Record) error {
		got = append(got, recs...)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("delivered %d records, want 3", len(got))
	}
	for i, r := range got {
		if r.Offset != offset.Offset(i) {
			t.Errorf("record %d offset = %v, want %v", i, r.Offset, i)
		}
	}
}

func TestSubscriberResumesFromCursor(t *testing.T) {
	s, _, store, lg := newTestSubscriber(t)
	appendRecords(t, lg, 5)
	if err := store.SaveCursor(context.Background(), "worker", mustName(t, "orders"), partition.ID(0), offset.Offset(3)); err != nil {
		t.Fatalf("SaveCursor() error = %v", err)
	}

	var got []message.Record
	err := s.Subscribe(context.Background(), consumer.Subscription{
		Consumer:  "worker",
		Topic:     mustName(t, "orders"),
		Partition: 0,
	}, func(recs []message.Record) error {
		got = append(got, recs...)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if len(got) != 2 || got[0].Offset != offset.Offset(3) || got[1].Offset != offset.Offset(4) {
		t.Fatalf("delivered offsets = %v, want [3 4]", offsetsOf(got))
	}
}

func TestSubscriberExplicitStartWinsOverCursor(t *testing.T) {
	s, _, store, lg := newTestSubscriber(t)
	appendRecords(t, lg, 5)
	if err := store.SaveCursor(context.Background(), "worker", mustName(t, "orders"), partition.ID(0), offset.Offset(3)); err != nil {
		t.Fatalf("SaveCursor() error = %v", err)
	}

	var got []message.Record
	err := s.Subscribe(context.Background(), consumer.Subscription{
		Consumer:  "worker",
		Topic:     mustName(t, "orders"),
		Partition: 0,
		Start:     offsetPtr(1),
	}, func(recs []message.Record) error {
		got = append(got, recs...)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if len(got) != 4 || got[0].Offset != offset.Offset(1) {
		t.Fatalf("delivered offsets = %v, want starting at 1", offsetsOf(got))
	}
}

func TestSubscriberStartBeyondEndFails(t *testing.T) {
	s, _, _, lg := newTestSubscriber(t)
	appendRecords(t, lg, 2)
	err := s.Subscribe(context.Background(), consumer.Subscription{
		Topic:     mustName(t, "orders"),
		Partition: 0,
		Start:     offsetPtr(9),
	}, func([]message.Record) error { return nil })
	if !errors.Is(err, offset.ErrOutOfRange) {
		t.Fatalf("Subscribe() error = %v, want ErrOutOfRange", err)
	}
}

func TestSubscriberFollowBlocksUntilCancel(t *testing.T) {
	s, _, _, lg := newTestSubscriber(t)
	appendRecords(t, lg, 1)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(ctx, consumer.Subscription{Topic: mustName(t, "orders"), Partition: 0, Follow: true}, func([]message.Record) error { return nil })
	}()
	select {
	case err := <-done:
		t.Fatalf("Subscribe() returned early with %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe() did not return after cancel")
	}
}

func TestSubscriberEmitErrorAborts(t *testing.T) {
	s, _, _, lg := newTestSubscriber(t)
	appendRecords(t, lg, 2)
	sentinel := errors.New("consumer aborted")
	err := s.Subscribe(context.Background(), consumer.Subscription{Topic: mustName(t, "orders"), Partition: 0}, func([]message.Record) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Subscribe() error = %v, want %v", err, sentinel)
	}
}

func TestSubscriberMissingPartition(t *testing.T) {
	s, _, _, _ := newTestSubscriber(t)
	err := s.Subscribe(context.Background(), consumer.Subscription{Topic: mustName(t, "orders"), Partition: 1}, func([]message.Record) error { return nil })
	if !errors.Is(err, partition.ErrNotFound) {
		t.Fatalf("Subscribe() error = %v, want partition.ErrNotFound", err)
	}
}

func TestAckAdvancesMonotonically(t *testing.T) {
	s, _, store, _ := newTestSubscriber(t)
	name := mustName(t, "orders")
	pid := partition.ID(0)

	got, err := s.Ack(context.Background(), "worker", name, pid, offset.Offset(5))
	if err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if got != offset.Offset(5) {
		t.Fatalf("Ack() = %v, want 5", got)
	}
	if c, ok, _ := store.GetCursor(context.Background(), "worker", name, pid); !ok || c != offset.Offset(5) {
		t.Fatalf("cursor = %v, %v; want 5, true", c, ok)
	}

	// Regression is ignored, current cursor is returned.
	got, err = s.Ack(context.Background(), "worker", name, pid, offset.Offset(3))
	if err != nil {
		t.Fatalf("Ack(regression) error = %v", err)
	}
	if got != offset.Offset(5) {
		t.Fatalf("Ack(regression) = %v, want current cursor 5", got)
	}

	// Advance past is accepted.
	got, err = s.Ack(context.Background(), "worker", name, pid, offset.Offset(9))
	if err != nil || got != offset.Offset(9) {
		t.Fatalf("Ack(advance) = %v, %v; want 9, nil", got, err)
	}
}

func TestAckValidatesInput(t *testing.T) {
	s, _, _, _ := newTestSubscriber(t)
	name := mustName(t, "orders")
	pid := partition.ID(0)

	if _, err := s.Ack(context.Background(), "bad/name", name, pid, offset.Offset(1)); !errors.Is(err, consumer.ErrInvalidName) {
		t.Fatalf("Ack(bad consumer) error = %v, want ErrInvalidName", err)
	}
	if _, err := s.Ack(context.Background(), "w", mustName(t, "nope"), pid, offset.Offset(1)); !errors.Is(err, topic.ErrNotFound) {
		t.Fatalf("Ack(bad topic) error = %v, want topic.ErrNotFound", err)
	}
	if _, err := s.Ack(context.Background(), "w", name, pid, offset.Invalid); err == nil {
		t.Fatal("Ack(invalid offset) error = nil, want error")
	}
}

func offsetsOf(recs []message.Record) []int64 {
	out := make([]int64, len(recs))
	for i, r := range recs {
		out[i] = r.Offset.Int64()
	}
	return out
}

func offsetPtr(o offset.Offset) *offset.Offset { return &o }

func appendRecords(t *testing.T, lg *fakeLog, n int) {
	t.Helper()
	recs := make([]message.Record, 0, n)
	for i := 0; i < n; i++ {
		recs = append(recs, message.Record{Message: message.Message{Payload: []byte{byte('a' + i)}}})
	}
	b := &message.RecordBatch{Records: recs}
	if _, err := lg.Append(context.Background(), b); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}
