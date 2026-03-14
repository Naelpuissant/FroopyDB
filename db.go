// Package froopydb main db file
package froopydb

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"

	"froopydb/compact"
	"froopydb/logger"
	"froopydb/table"
	"froopydb/wal"
)

var (
	KB = 1024
	MB = 1024 * KB
)

type DBMetrics struct {
	TotalKeys    int `json:"totalKeys"`
	MemTableKeys int `json:"memTableKeys"`
	SSTableKeys  int `json:"sstableKeys"`
	NumSSTables  int `json:"numSSTables"`
	MemTableSize int `json:"memTableSize"`
	DiskStorage  int `json:"diskStorage"`
	PendingFlush int `json:"pendingFlush"`
}

type DBConfig struct {
	Folder          string
	MemTableMaxSize int
	ClearOnStart    bool
	LogLevel        int
}

func DefaultConfig(folder string) *DBConfig {
	return &DBConfig{
		Folder:          folder,
		MemTableMaxSize: 64 * MB,
		ClearOnStart:    false,
		LogLevel:        logger.INFO,
	}
}

// Table represents the database state
// immmutable memtable (imm) and SStableStore (sst) are both immutable (CoW)
type Tables struct {
	mem *table.MemTable
	imm []*table.MemTable
	sst *table.SSTableStore
}

func NewTables(mem *table.MemTable, imm []*table.MemTable, sst *table.SSTableStore) *Tables {
	return &Tables{
		mem: mem,
		imm: imm,
		sst: sst,
	}
}

type DB struct {
	logger     *logger.Logger
	folder     string
	TxnManager *TxnManager
	tables     atomic.Pointer[Tables]
	flushJobs  chan *Tables
	wg         sync.WaitGroup
	mu         sync.RWMutex
}

func NewDB(config *DBConfig) *DB {
	if config.ClearOnStart {
		err := os.RemoveAll(config.Folder)
		if err != nil {
			panic(err)
		}
	}

	logger := logger.NewLogger(config.LogLevel)

	err := os.MkdirAll(config.Folder, 0o777)
	if err != nil {
		panic(err)
	}

	memTable := table.NewMemTable(
		logger,
		config.MemTableMaxSize,
		wal.NewWAL(config.Folder, false),
	)

	sstables, err := table.NewSSTableStore(logger, config.Folder)
	if err != nil {
		logger.Error("Failed to create SSTable store", "error", err)
		panic(err)
	}

	txnManager := NewTxnManager()
	db := &DB{
		logger:     logger,
		folder:     config.Folder,
		TxnManager: txnManager,
		flushJobs:  make(chan *Tables, 10),
	}

	tables := NewTables(
		memTable,
		[]*table.MemTable{},
		sstables,
	)
	db.tables.Store(tables)

	go db.flushWorker()

	return db
}

func (db *DB) Close() {
	tables := db.tables.Load()
	tables.sst.CloseAll()
}

func (db *DB) NewTransaction() *Txn {
	return NewTxn(db)
}

// Set inserts or updates a key-value pair in the database
// and returns the value that was set.
// If the memtable exceeds its max size after the set,
// it will be flushed to disk in the background.
func (db *DB) Set(key []byte, value []byte) []byte {
	for {
		tables := db.tables.Load()
		if !tables.mem.ShouldFlush(key, value) {
			tables.mem.Set(key, value)
			return value
		}

		// Check if it's just an update (key already exists)
		_, found := tables.mem.Get(key)
		if found {
			tables.mem.Set(key, value)
			return value
		}

		// try flushing
		newImm := db.appendImmMemTable(tables.imm, tables.mem)
		newMem := table.NewMemTable(db.logger, tables.mem.MaxSize(), wal.NewWAL(db.folder, false))
		newTables := NewTables(newMem, newImm, tables.sst)
		if db.tables.CompareAndSwap(tables, newTables) {
			tables.mem.SetLoggerImmutable()
			db.wg.Add(1)
			db.flushJobs <- tables
			newMem.Set(key, value)
			return value
		}
	}
}

func (db *DB) getFromImm(keyBytes []byte) ([]byte, bool) {
	immMemTables := db.tables.Load().imm
	if len(immMemTables) > 0 {
		for i := len(immMemTables) - 1; i >= 0; i-- {
			value, found := immMemTables[i].Get(keyBytes)
			if found {
				return value, true
			}
		}
	}
	return []byte{}, false
}

// Get retrieves the value/found for a given key
func (db *DB) Get(key []byte) ([]byte, bool) {
	tables := db.tables.Load()
	if value, found := tables.mem.Get(key); found {
		return value, true
	}

	if value, found := db.getFromImm(key); found {
		return value, true
	}

	if value, found := tables.sst.Search(key); found {
		return value, true
	}

	return nil, false
}

// Delete marks a key as deleted by setting its value to a tombstone (0x00)
func (db *DB) Delete(key []byte) {
	_ = db.Set(key, []byte{0x00})
}

// Range retrieves all key-value pairs in the specified key range [fromKey, toKey]
// and returns them as a skiplist
func (db *DB) Range(fromKey []byte, toKey []byte) map[string][]byte {
	if bytes.Compare(fromKey, toKey) > 0 {
		return map[string][]byte{}
	}

	result := map[string][]byte{}

	tables := db.tables.Load()
	tables.sst.Range(result, fromKey, toKey)
	if len(tables.imm) > 0 {
		for i := len(tables.imm) - 1; i >= 0; i-- {
			tables.imm[i].Range(result, fromKey, toKey)
		}
	}
	tables.mem.Range(result, fromKey, toKey)

	return result
}

// MemTableSize returns the current size of the MemTable in bytes
func (db *DB) MemTableSize() int {
	tables := db.tables.Load()
	return tables.mem.Size()
}

// NumSSTables returns the number of SSTables currently in the database
func (db *DB) NumSSTables() int {
	tables := db.tables.Load()
	return tables.sst.Len()
}

// Metrics returns a json format of the database metrics
// metrics include total keys, number of SSTables, MemTable size, SSTables size
func (db *DB) Metrics() DBMetrics {
	// TODO : use atomic counters for these metrics instead
	// of locking for better performance
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := db.tables.Load()

	memTableKeys := tables.mem.Len()
	sstKeys := tables.sst.TotalKeys()
	numSST := tables.sst.Len()
	memTableSize := db.MemTableSize()
	diskStorage := tables.sst.TotalSize()
	pendingFlush := len(tables.imm)

	return DBMetrics{
		TotalKeys:    memTableKeys + sstKeys,
		MemTableKeys: memTableKeys,
		SSTableKeys:  sstKeys,
		NumSSTables:  numSST,
		MemTableSize: memTableSize,
		DiskStorage:  diskStorage,
		PendingFlush: pendingFlush,
	}
}

// Compact triggers a manual compaction of SSTables.
func (db *DB) Compact() {
	db.mu.Lock()
	defer db.mu.Unlock()
	tables := db.tables.Load()
	// TODO : Send tables state
	compact.MaybeCompactToUpperLevel(tables.sst)
	compact.MaybeCompactL0(tables.sst)
}

// WaitJobs waits for all pending flush and compaction jobs to complete
func (db *DB) WaitJobs() {
	db.wg.Wait()
}

func (db *DB) flushWorker() {
	for old := range db.flushJobs {
		for {
			tables := db.tables.Load()

			newTable := table.NewSSTable(db.folder, 0, old.sst.Len(), true, 0)
			_, err := newTable.Open()
			if err != nil {
				db.logger.Error("Failed to open new SSTable", "error", err)
			}

			err = old.mem.Flush(newTable)
			if err != nil {
				db.logger.Error("Failed to flush memtable", "error", err)
			}

			newSST := tables.sst.ImmutableAdd(newTable)
			newImm := db.removeImmMemTable(tables.imm, old.mem)
			newTables := NewTables(tables.mem, newImm, newSST)
			if db.tables.CompareAndSwap(tables, newTables) {
				break
			}
		}
		db.wg.Done()
	}
}

// removeImmMemTable removes the given memtable and returns the new imm memtable slice without it
func (db *DB) removeImmMemTable(imm []*table.MemTable, mt *table.MemTable) []*table.MemTable {
	newImmMemTables := make([]*table.MemTable, 0, len(imm)-1)

	for _, m := range imm {
		if m != mt {
			newImmMemTables = append(newImmMemTables, m)
		}
	}

	return newImmMemTables
}

// appendImmMemTable appends the given memtable to the imm memtable slice
// and returns the new imm memtable slice without it
func (db *DB) appendImmMemTable(imm []*table.MemTable, mt *table.MemTable) []*table.MemTable {
	return append(append([]*table.MemTable{}, imm...), mt)
}
