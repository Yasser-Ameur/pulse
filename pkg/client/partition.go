package client

import "hash/fnv"

// PartitionForKey is the client-side routing used when a topic has several
// partitions: it hashes key with 32-bit FNV-1a and reduces it modulo
// partitions, so the same key always routes to the same partition for a
// given partition count. partitions <= 0 returns 0. The mapping is stable
// across releases; do not change the hash algorithm.
func PartitionForKey(key string, partitions int) int32 {
	if partitions <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int32(h.Sum32() % uint32(partitions))
}
