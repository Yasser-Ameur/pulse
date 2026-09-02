package services

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pulse-stream/pulse/internal/application/ports"
	"github.com/pulse-stream/pulse/internal/domain/broker"
	"github.com/pulse-stream/pulse/internal/domain/consumer"
	"github.com/pulse-stream/pulse/internal/domain/message"
	"github.com/pulse-stream/pulse/internal/domain/offset"
	"github.com/pulse-stream/pulse/internal/domain/partition"
	"github.com/pulse-stream/pulse/internal/domain/retention"
	"github.com/pulse-stream/pulse/internal/domain/storage"
	"github.com/pulse-stream/pulse/internal/domain/topic"
)

// fakeClock is a deterministic Clock.
type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

// fakeLogger records log lines for assertions.
type fakeLogger struct {
	mu    sync.Mutex
	infos []string
	warns []string
	errs  []string
}

func (l *fakeLogger) Debug(_ string, _ ...ports.Field) {}
func (l *fakeLogger) Info(msg string, _ ...ports.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, msg)
}
func (l *fakeLogger) Warn(msg string, _ ...ports.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}
func (l *fakeLogger) Error(msg string, _ ...ports.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, msg)
}
func (l *fakeLogger) With(_ ...ports.Field) ports.Logger { return l }

// fakeMetrics is a no-op MetricsRecorder.
type fakeMetrics struct{}

func (fakeMetrics) RecordPublish(int, int)             {}
func (fakeMetrics) RecordConsume(int, int)             {}
func (fakeMetrics) RecordPublishLatency(time.Duration) {}
func (fakeMetrics) RecordConsumeLatency(time.Duration) {}
func (fakeMetrics) RecordBytesWritten(int64)           {}
func (fakeMetrics) RecordBytesRead(int64)              {}

// fakeLog is an in-memory storage.Log.
type fakeLog struct {
	mu         sync.Mutex
	records    []message.Record
	closed     bool
	synced     int
	appendErr  error
	notify     chan struct{}
	trimCalls  int
	trimErr    error
	trimResult retention.TrimResult
	lastPolicy retention.Policy

	compactCalls  int
	compactErr    error
	compactResult storage.CompactionResult
}

func newFakeLog() *fakeLog { return &fakeLog{notify: make(chan struct{})} }

func (l *fakeLog) Append(_ context.Context, batch *message.RecordBatch) (offset.Offset, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.appendErr != nil {
		return offset.Invalid, l.appendErr
	}
	base := offset.Offset(len(l.records))
	for i := range batch.Records {
		batch.Records[i].Offset = base + offset.Offset(i)
	}
	l.records = append(l.records, batch.Records...)
	batch.BaseOffset = base
	close(l.notify)
	l.notify = make(chan struct{})
	return base, nil
}

func (l *fakeLog) Read(_ context.Context, from offset.Offset, limit, maxBytes int) ([]message.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !from.Valid() {
		return nil, offset.ErrInvalid
	}
	if int(from) >= len(l.records) {
		return nil, nil
	}
	out := make([]message.Record, 0, len(l.records)-int(from))
	bytes := 0
	for i := int(from); i < len(l.records); i++ {
		if limit > 0 && len(out) >= limit {
			break
		}
		if maxBytes > 0 && len(out) > 0 && bytes+len(l.records[i].Message.Payload) > maxBytes {
			break
		}
		out = append(out, l.records[i])
		bytes += len(l.records[i].Message.Payload)
	}
	return out, nil
}

func (l *fakeLog) NextOffset() offset.Offset {
	l.mu.Lock()
	defer l.mu.Unlock()
	return offset.Offset(len(l.records))
}

func (l *fakeLog) Notify() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.notify
}

func (l *fakeLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.synced++
	return nil
}

func (l *fakeLog) Trim(_ time.Time, p retention.Policy) (retention.TrimResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.trimCalls++
	l.lastPolicy = p
	return l.trimResult, l.trimErr
}

func (l *fakeLog) Compact(_ context.Context) (storage.CompactionResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.compactCalls++
	return l.compactResult, l.compactErr
}

func (l *fakeLog) Truncate(to offset.Offset) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = l.records[:to]
	return nil
}

func (l *fakeLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	return nil
}

// fakeStore is an in-memory MetadataStore.
type fakeStore struct {
	mu        sync.Mutex
	topics    map[topic.Name]topic.Topic
	cursors   map[cursorKey]offset.Offset
	clusterID broker.ClusterID
	brokerID  broker.BrokerID
	closed    bool
}

type cursorKey struct {
	consumer  consumer.ID
	topic     topic.Name
	partition partition.ID
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		topics:  make(map[topic.Name]topic.Topic),
		cursors: make(map[cursorKey]offset.Offset),
	}
}

func (s *fakeStore) CreateTopic(_ context.Context, t topic.Topic) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[t.Name]; ok {
		return topic.ErrAlreadyExists
	}
	s.topics[t.Name] = t
	return nil
}

func (s *fakeStore) GetTopic(_ context.Context, name topic.Name) (topic.Topic, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.topics[name]
	return t, ok, nil
}

func (s *fakeStore) DeleteTopic(_ context.Context, name topic.Name) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[name]; !ok {
		return topic.ErrNotFound
	}
	delete(s.topics, name)
	return nil
}

func (s *fakeStore) ListTopics(_ context.Context) ([]topic.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]topic.Topic, 0, len(s.topics))
	for _, t := range s.topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *fakeStore) SaveCursor(_ context.Context, c consumer.ID, t topic.Name, p partition.ID, o offset.Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors[cursorKey{c, t, p}] = o
	return nil
}

func (s *fakeStore) GetCursor(_ context.Context, c consumer.ID, t topic.Name, p partition.ID) (offset.Offset, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.cursors[cursorKey{c, t, p}]
	return o, ok, nil
}

func (s *fakeStore) CreateCluster(_ context.Context, id broker.ClusterID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusterID = id
	return nil
}

func (s *fakeStore) ClusterID(_ context.Context) (broker.ClusterID, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clusterID, s.clusterID != "", nil
}

func (s *fakeStore) CreateBroker(_ context.Context, id broker.BrokerID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.brokerID = id
	return nil
}

func (s *fakeStore) BrokerID(_ context.Context) (broker.BrokerID, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.brokerID, s.brokerID != "", nil
}

func (s *fakeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// fakeFactory is an in-memory LogFactory.
type fakeFactory struct {
	mu        sync.Mutex
	logs      map[partitionKey]*fakeLog
	created   []partitionKey
	deleted   []partitionKey
	createErr error
	openErr   error
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{logs: make(map[partitionKey]*fakeLog)}
}

func (f *fakeFactory) Create(_ context.Context, name topic.Name, id partition.ID) (storage.Log, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}
	k := partitionKey{topicName: name, partition: id}
	lg := newFakeLog()
	f.logs[k] = lg
	f.created = append(f.created, k)
	return lg, nil
}

func (f *fakeFactory) Open(_ context.Context, name topic.Name, id partition.ID) (storage.Log, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.openErr != nil {
		return nil, f.openErr
	}
	lg, ok := f.logs[partitionKey{topicName: name, partition: id}]
	if !ok {
		return nil, ports.ErrLogNotFound
	}
	return lg, nil
}

func (f *fakeFactory) Delete(_ context.Context, name topic.Name, id partition.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := partitionKey{topicName: name, partition: id}
	if _, ok := f.logs[k]; ok {
		delete(f.logs, k)
		f.deleted = append(f.deleted, k)
	}
	return nil
}

// mustName constructs a validated topic name, failing the test on error.
func mustName(t testing.TB, s string) topic.Name {
	t.Helper()
	n, err := topic.NewName(s)
	if err != nil {
		t.Fatalf("NewName(%q) error = %v", s, err)
	}
	return n
}
