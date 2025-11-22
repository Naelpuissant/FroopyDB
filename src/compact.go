package src

func Compact(tables []*SSTable, target *SSTable) *SSTable {
	compactedTable := map[[4]byte][]byte{}

	for _, table := range tables {
		table.file.Seek(0, 0)
		for idx := range table.index {
			compactedTable[idx] = table.Search(idx)
		}
	}

	// persist in new tmp file with max incr file name
	tmpSegment := NewSSTable(target.folder, 1, target.incr, true, 0)
	tmpSegment.Open()

	for key, value := range compactedTable {
		tmpSegment.WriteBlock(key, value)
	}
	tmpSegment.WriteIndices()

	return tmpSegment
}
