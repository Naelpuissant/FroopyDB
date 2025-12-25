package froopydb

import (
	"froopydb/compact"
	t "froopydb/table"
	"froopydb/wal"
	"froopydb/x"
	"os"
	"sync"
)

var (
	KB = 1024
	MB = 1024 * KB
)

type DB struct {
	folder   string
	sstables *t.SSTableStore

	memTable     *t.MemTable
	immMu        sync.Mutex
	immMemTables []*t.MemTable
	flushJobs    chan *t.MemTable
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

	logger := wal.NewWAL(folder, false)

	memTable := t.NewMemTable(
		memTableMaxSize,
		logger,
	)

	sstables := t.NewSSTableStore(folder, sstableMaxSize)

	db := &DB{
		folder:       folder,
		memTable:     memTable,
		immMemTables: []*t.MemTable{},
		sstables:     sstables,
		flushJobs:    make(chan *t.MemTable),
	}

	go db.flushWorker()

	return db
}

func (db *DB) Close() {
	db.sstables.CloseAll()
}

func (db *DB) Set(key int, value string) string {
	keyBytes := x.Uint32ToBytes(uint32(key))
	valueBytes := x.StrToBytes(value)

	if db.memTable.ShouldFlush(keyBytes, valueBytes) {
		old := db.memTable
		db.flushJobs <- old
		db.memTable = t.NewMemTable(old.MaxSize(), wal.NewWAL(db.folder, false))
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
	keyBytes := x.Uint32ToBytes(uint32(key))

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
	db.sstables.DeleteIndex(([4]byte)(x.Uint32ToBytes(uint32(key))))
	return line
}

func (db *DB) flushWorker() {
	for mt := range db.flushJobs {
		db.immMemTables = append(db.immMemTables, mt)
		mt.SetLoggerImmutable()

		newTable := t.NewSSTable(db.folder, 0, db.sstables.Len(), true, 0)
		newTable.Open()
		mt.Flush(newTable)
		db.sstables.Add(newTable)

		db.removeImmMemTable(mt)

		// Compact
		compact.MaybeCompactToUpperLevel(db.sstables)
		compact.MaybeCompactL0(db.sstables)
	}
}

func (db *DB) removeImmMemTable(mt *t.MemTable) {
	db.immMu.Lock()
	defer db.immMu.Unlock()

	newImmMemTables := []*t.MemTable{}
	for _, immt := range db.immMemTables {
		if immt != mt {
			newImmMemTables = append(newImmMemTables, mt)
		}
	}
	db.immMemTables = newImmMemTables
}

func (db *DB) ImmMemTables() []*t.MemTable {
	return db.immMemTables
}
