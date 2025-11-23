package src

import (
	"os"
	"sync"
)

var (
	KB = 1024
	MB = 1024 * KB
)

type DB struct {
	folder   string
	sstables *SSTableStore

	memTable     *MemTable
	immMu        sync.Mutex
	immMemTables []*MemTable
	flushJobs    chan *MemTable
}

func NewDB(folder string, sstableMaxSize int, memTableMaxSize int, clearOnStart bool) *DB {
	if sstableMaxSize == 0 {
		sstableMaxSize = 1000
	}

	if memTableMaxSize == 0 {
		memTableMaxSize = 64 * MB
	}

	if clearOnStart {
		os.RemoveAll(folder)
	}
	os.MkdirAll(folder, 0777)

	logger := NewWAL(folder, false)

	memTable := NewMemTable(
		memTableMaxSize,
		logger,
	)

	sstables := NewSSTableStore(folder, sstableMaxSize)

	maxFlushJobs := 5
	db := &DB{
		folder:       folder,
		memTable:     memTable,
		immMemTables: []*MemTable{},
		sstables:     sstables,
		flushJobs:    make(chan *MemTable, maxFlushJobs),
	}

	go db.flushWorker()

	return db
}

func (db *DB) Close() {
	db.sstables.CloseAll()
}

func (db *DB) Set(key int, value string) string {
	keyBytes := Uint32ToBytes(uint32(key))
	valueBytes := StrToBytes(value)

	if db.memTable.ShouldFlush(keyBytes, valueBytes) {
		old := db.memTable
		db.flushJobs <- old
		db.memTable = NewMemTable(old.maxSize, NewWAL(db.folder, false))

		// db.sstables.MaybeCompactToUpperLevel()
		// db.sstables.MaybeCompactL0()
	}
	db.memTable.Set(keyBytes, valueBytes)
	return value
}

func (db *DB) getFromImm(keyBytes []byte) ([]byte, bool) {
	if len(db.immMemTables) > 0 {
		for i := len(db.immMemTables) - 1; i >= 0; i-- {
			value, found := db.immMemTables[i].Get(keyBytes)
			if found {
				return value, true
			}
		}
	}
	return []byte{}, false
}

func (db *DB) Get(key int) string {
	keyBytes := Uint32ToBytes(uint32(key))

	value, found := db.memTable.Get(keyBytes)
	if found {
		return string(value)
	}

	value, found = db.getFromImm(keyBytes)
	if found {
		return string(value)
	}

	return string(db.sstables.Search(([4]byte)(keyBytes)))
}

func (db *DB) Delete(key int) string {
	line := db.Set(key, "\x00")
	db.sstables.DeleteIndex(([4]byte)(Uint32ToBytes(uint32(key))))
	return line
}

func (db *DB) flushWorker() {
	for mt := range db.flushJobs {
		db.immMemTables = append(db.immMemTables, mt)
		mt.logger.Immutable()
		newTable := NewSSTable(db.folder, 0, db.sstables.Len(), true, 0)
		newTable.Open()
		mt.Flush(newTable)
		db.removeImmMemTable(mt)
	}
}

func (db *DB) removeImmMemTable(mt *MemTable) {
	db.immMu.Lock()
	defer db.immMu.Unlock()

	newImmMemTables := []*MemTable{}
	for _, immt := range db.immMemTables {
		if immt != mt {
			newImmMemTables = append(newImmMemTables, mt)
		}
	}
	db.immMemTables = newImmMemTables
}

func (db *DB) ImmMemTables() []*MemTable {
	return db.immMemTables
}
