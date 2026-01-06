package froopydb

import (
	"froopydb/compact"
	"froopydb/logger"
	"froopydb/table"
	"froopydb/wal"
	"os"
	"sync"
)

var (
	KB = 1024
	MB = 1024 * KB
)

type DB struct {
	logger   *logger.Logger
	folder   string
	sstables *table.SSTableStore
	memTable *table.MemTable

	immMu        sync.Mutex
	immMemTables []*table.MemTable
	flushJobs    chan *table.MemTable
}

func NewDB(folder string, sstableMaxSize int, memTableMaxSize int, clearOnStart bool, logLevel int) *DB {
	if sstableMaxSize == 0 {
		sstableMaxSize = 1000
	}

	if memTableMaxSize == 0 {
		memTableMaxSize = 64 * MB
	}

	if clearOnStart {
		os.RemoveAll(folder)
	}

	logger := logger.NewLogger(logLevel)

	os.MkdirAll(folder, 0777)

	memTable := table.NewMemTable(
		logger,
		memTableMaxSize,
		wal.NewWAL(folder, false),
	)

	sstables, err := table.NewSSTableStore(logger, folder, sstableMaxSize)
	if err != nil {
		logger.Error("Failed to create SSTable store", "error", err)
		panic(err)
	}

	db := &DB{
		logger:       logger,
		folder:       folder,
		memTable:     memTable,
		immMemTables: []*table.MemTable{},
		sstables:     sstables,
		flushJobs:    make(chan *table.MemTable),
	}

	go db.flushWorker()

	return db
}

func (db *DB) Close() {
	db.sstables.CloseAll()
}

func (db *DB) Set(key []byte, value []byte) []byte {
	if db.memTable.ShouldFlush(key, value) {
		old := db.memTable
		db.flushJobs <- old
		db.memTable = table.NewMemTable(db.logger, old.MaxSize(), wal.NewWAL(db.folder, false))
	}
	db.memTable.Set(key, value)
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

func (db *DB) Get(key []byte) string {
	value, found := db.memTable.Get(key)
	if found {
		return string(value)
	}

	value, found = db.getFromImm(key)
	if found {
		return string(value)
	}

	value, err := db.sstables.Search(key)
	if err != nil {
		println(err)
	}

	return string(value)
}

func (db *DB) Delete(key []byte) []byte {
	line := db.Set(key, []byte{0x00})
	db.sstables.DeleteIndex(key)
	return line
}

func (db *DB) flushWorker() {
	for mt := range db.flushJobs {
		db.immMemTables = append(db.immMemTables, mt)
		mt.SetLoggerImmutable()

		newTable := table.NewSSTable(db.folder, 0, db.sstables.Len(), true, 0)
		newTable.Open()
		mt.Flush(newTable)
		db.sstables.Add(newTable)

		db.removeImmMemTable(mt)

		// Compact -> should be called by the user ?
		compact.MaybeCompactToUpperLevel(db.sstables)
		compact.MaybeCompactL0(db.sstables)
	}
}

func (db *DB) removeImmMemTable(mt *table.MemTable) {
	db.immMu.Lock()
	defer db.immMu.Unlock()

	newImmMemTables := []*table.MemTable{}
	for _, immt := range db.immMemTables {
		if immt != mt {
			newImmMemTables = append(newImmMemTables, mt)
		}
	}
	db.immMemTables = newImmMemTables
}

func (db *DB) ImmMemTables() []*table.MemTable {
	return db.immMemTables
}
