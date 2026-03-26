package compact

import (
	t "froopydb/table"
	"maps"
)

func doCompact(tables []*t.SSTable, target *t.SSTable) *t.SSTable {
	compactedTable := map[string][]byte{}

	for _, table := range tables {
		maps.Insert(compactedTable, table.KVIter())
	}

	// persist in new tmp file with max incr file name
	tmpSegment := t.NewSSTable(target.Folder(), 1, target.Incr(), true, 0)
	tmpSegment.Open()

	tmpSegment.InitWriter()
	for key, value := range compactedTable {
		tmpSegment.WriteDataBlock([]byte(key), value)
	}
	idxOffset, _ := tmpSegment.WriteIndex()
	bfOffset, _ := tmpSegment.WriteBloomFilter()
	tmpSegment.WriteMetadata(idxOffset, bfOffset)
	tmpSegment.FlushWriter()
	tmpSegment.Ready()

	return tmpSegment
}

func MaybeCompact(store *t.SSTableStore) map[int][]*t.SSTable {
	threshold := 3
	maxLevel := 2
	newTables := map[int][]*t.SSTable{}

	for levelKey, tables := range store.Tables() {
		// Ignore levels that are above the max level for compaction
		if levelKey >= maxLevel {
			newTables[levelKey] = tables
			continue
		}
		for i := 0; i+threshold-1 < len(tables); i += threshold {
			tablesToCompact := tables[i : i+threshold]
			newTable := doCompact(tablesToCompact, tables[i+threshold-1])
			deleteTables(tablesToCompact)
			newTables[levelKey+1] = append(newTables[levelKey+1], newTable)
		}
		if len(tables)%threshold != 0 {
			remainTables := tables[len(tables)-len(tables)%threshold:]
			newTables[levelKey] = append(newTables[levelKey], remainTables...)
		}
	}

	return newTables
}

// TODO : Table deletion should be done once
// we are sure that every reads has finished
// basically 2 solutions :
// - Wait n sec and delete
// - table pointer trigger deletion when reached 0
// first is fine for now
func deleteTables(tables []*t.SSTable) {
	for _, table := range tables {
		table.Remove()
	}
}
