package log

import (
	"os"
	"testing"

	"github.com/Yasser-Ameur/pulse/internal/domain/partition"
	"github.com/Yasser-Ameur/pulse/internal/domain/topic"
	"github.com/Yasser-Ameur/pulse/internal/infrastructure/storage/filesystem"
)

// TestDeleteLogRemovesEmptyTopicDir pins that deleting the last partition also
// removes the topic directory, while a sibling partition keeps it alive.
func TestDeleteLogRemovesEmptyTopicDir(t *testing.T) {
	root := t.TempDir()
	name, err := topic.NewName("orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range []partition.ID{0, 1} {
		if err := os.MkdirAll(filesystem.PartitionDir(root, name, pid), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	if err := DeleteLog(root, name, 0); err != nil {
		t.Fatalf("DeleteLog(0) error = %v", err)
	}
	if _, err := os.Stat(filesystem.TopicDir(root, name)); err != nil {
		t.Fatalf("topic dir removed while partition 1 remains: %v", err)
	}

	if err := DeleteLog(root, name, 1); err != nil {
		t.Fatalf("DeleteLog(1) error = %v", err)
	}
	if _, err := os.Stat(filesystem.TopicDir(root, name)); !os.IsNotExist(err) {
		t.Fatalf("topic dir still present after last partition deleted: %v", err)
	}
}
