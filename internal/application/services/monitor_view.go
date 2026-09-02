package services

import "github.com/Yasser-Ameur/pulse/internal/domain/partition"

// PartitionView is a read-only snapshot of a single partition's offsets, for
// the monitor HTTP listener's /varz endpoint.
type PartitionView struct {
	ID        int   `json:"id"`
	EndOffset int64 `json:"end_offset"`
}

// TopicView is a read-only snapshot of a topic and its partitions, for the
// monitor HTTP listener's /varz endpoint.
type TopicView struct {
	Name       string          `json:"name"`
	Partitions []PartitionView `json:"partitions"`
}

// TopicsView returns a snapshot of every registered topic and its
// partitions' offsets. It never blocks on the broker lifecycle lock and is
// safe to call in any state. Only the log end is reported: the storage.Log
// port exposes NextOffset and no low-water mark.
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
			partitions = append(partitions, PartitionView{ID: p, EndOffset: end})
		}
		views = append(views, TopicView{Name: string(t.Name), Partitions: partitions})
	}
	return views
}
