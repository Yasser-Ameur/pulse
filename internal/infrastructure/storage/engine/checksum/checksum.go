// Package checksum provides the CRC-32C (Castagnoli) checksum used to frame
// every record batch. CRC-32C is the standard for checksummed framing on
// modern hardware: it has strong error-detection properties and dedicated
// instruction support on x86 and ARM.
package checksum

import "hash/crc32"

// Table is the CRC-32C (Castagnoli) polynomial table.
var Table = crc32.MakeTable(crc32.Castagnoli)

// Sum computes the CRC-32C checksum of data.
func Sum(data []byte) uint32 {
	return crc32.Checksum(data, Table)
}
