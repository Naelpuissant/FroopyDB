package compact

import (
	t "froopydb/table"
	"strings"
)

func doCompact(tables []*t.SSTable, target *t.SSTable) *t.SSTable {
	compactedTable := map[string][]byte{}

	for _, table := range tables {
		table.ResetFilePointer()
		for idx := range table.Index() {
			value, err := table.Search([]byte(idx))
			if err != nil {
				panic(err) // TODO: handle error properly
			}
			compactedTable[idx] = value
		}
	}

	// persist in new tmp file with max incr file name
	tmpSegment := t.NewSSTable(target.Folder(), 1, target.Incr(), true, 0)
	tmpSegment.Open()

	for key, value := range compactedTable {
		tmpSegment.WriteBlock([]byte(key), value)
	}
	tmpSegment.WriteIndices()

	return tmpSegment
}

func MaybeCompactL0(store *t.SSTableStore) {
	threshold := 3

	tablesToCompact := []*t.SSTable{}
	tablesToDelete := []*t.SSTable{}
	tablesToReplace := [][2]*t.SSTable{}

	count := 0
	for levelKey, level := range store.Tables() {
		for _, table := range level {
			// for now I only handle l0 compaction but I should handle higher levels
			if levelKey == 0 {
				count++
				if count >= threshold {
					newTable := doCompact(append(tablesToCompact, table), table)
					tablesToDelete = append(tablesToDelete, tablesToCompact...)
					tablesToReplace = append(tablesToReplace, [2]*t.SSTable{table, newTable})
					count = 0
					tablesToCompact = []*t.SSTable{}
				} else {
					tablesToCompact = append(tablesToCompact, table)
				}
			}
		}
	}

	for _, table := range tablesToDelete {
		store.Remove(table)
	}

	for _, table := range tablesToReplace {
		table[1].Ready()
		store.Replace(table[0], table[1])
	}
}

func MaybeCompactToUpperLevel(store *t.SSTableStore) {
	tablesToDelete := []*t.SSTable{}
	tablesToReplace := [][2]*t.SSTable{}

	for i := range store.Tables() {
		// we are at the top level
		if i+1 > len(store.Tables()) {
			return
		}
		tablesToCompact := []*t.SSTable{}
		for _, l1 := range store.Tables()[i+1] {
			l1min, l1max := l1.GetMinMax()
			tablesToCompact = append(tablesToCompact, l1)
			for _, l0 := range store.Tables()[i] {
				l0min, l0max := l0.GetMinMax()
				if l0max >= l1min && l0min <= l1max {
					tablesToCompact = append(tablesToCompact, l0)
					tablesToDelete = append(tablesToDelete, l0)

					l1min = min(l0min, l1min)
					l1max = max(l0max, l1max)
				}
			}
			newTable := doCompact(tablesToCompact, l1)
			tablesToReplace = append(tablesToReplace, [2]*t.SSTable{l1, newTable})
		}
	}

	for _, table := range tablesToDelete {
		store.Remove(table)
	}

	for _, table := range tablesToReplace {
		store.Replace(table[0], table[1])
		table[1].Rename(strings.TrimSuffix(table[1].Name(), ".tmp"))
	}
}
