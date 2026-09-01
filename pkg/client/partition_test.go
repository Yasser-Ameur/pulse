package client_test

import (
	"testing"

	"github.com/pulse-stream/pulse/pkg/client"
)

// TestPartitionForKeyIsStable pins the FNV-1a routing mapping for three known
// keys: the algorithm must never change across releases, or already-routed
// keys would silently land on a different partition.
func TestPartitionForKeyIsStable(t *testing.T) {
	cases := []struct {
		key        string
		partitions int
		want       int32
	}{
		{"user-42", 4, 3},
		{"order-1001", 8, 4},
		{"pulse", 3, 1},
	}
	for _, tc := range cases {
		if got := client.PartitionForKey(tc.key, tc.partitions); got != tc.want {
			t.Errorf("PartitionForKey(%q, %d) = %d, want %d", tc.key, tc.partitions, got, tc.want)
		}
	}
}

func TestPartitionForKeyNonPositivePartitions(t *testing.T) {
	if got := client.PartitionForKey("any", 0); got != 0 {
		t.Errorf("PartitionForKey with 0 partitions = %d, want 0", got)
	}
	if got := client.PartitionForKey("any", -1); got != 0 {
		t.Errorf("PartitionForKey with negative partitions = %d, want 0", got)
	}
}
