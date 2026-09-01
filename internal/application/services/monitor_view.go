package services

import "github.com/pulse-stream/pulse/internal/domain/partition"

// PartitionView is a read-only snapshot of a single partition's offsets, for
// the monitor HTTP listener's /varz endpoint.
type PartitionView struct {
	ID          int
	StartOffset int64
	EndOffset   int64
}

// TopicView is a read-only snapshot of a topic and its partitions, for the
// monitor HTTP listener's /varz endpoint.
type TopicView struct {
	Name       string
	Partitions []PartitionView
}

// TopicsView returns a snapshot of every registered topic and its
// partitions' offsets. It never blocks on the broker lifecycle lock and is
// safe to call in any state.
//
// StartOffset is always 0: the storage.Log port exposes only NextOffset
// (the log end / LEO), not a low-water mark, so the trimmed start of a
// partition cannot be reported without adding that accessor to the log
// engine (owned by a sibling package).
func (b *Broker) TopicsView() []TopicView {
	topics := b.registry.Topics()
	views := make([]TopicView, 0, len(topics))
	for _, t := range topics {
		partitions := make([]PartitionView, 0, t.Partitions)
		for p := 0; p < t.Partitions; p++ {
			var end int64
			if lg, ok := b.registry.Log(t.Name, partition.ID(p)); ok {
				end = int64(lg.NextOffset())
			}
			partitions = append(partitions, PartitionView{ID: p, StartOffset: 0, EndOffset: end})
		}
		views = append(views, TopicView{Name: string(t.Name), Partitions: partitions})
	}
	return views
}
