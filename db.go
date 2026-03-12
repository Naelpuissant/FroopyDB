// Package froopydb main db file
package froopydb

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"

	"froopydb/compact"
	"froopydb/logger"
	"froopydb/skiplist"
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

type DB struct {
	logger     *logger.Logger
	folder     string
	TxnManager *TxnManager

	sstables *table.SSTableStore
	memTable *table.MemTable

	immMemTables atomic.Pointer[[]*table.MemTable]

	flushJobs chan *table.MemTable
	wg        sync.WaitGroup
	mu        sync.RWMutex
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
		logger:       logger,
		folder:       config.Folder,
		TxnManager:   txnManager,
		memTable:     memTable,
		immMemTables: atomic.Pointer[[]*table.MemTable]{},
		sstables:     sstables,
		flushJobs:    make(chan *table.MemTable),
	}
	db.immMemTables.Store(&[]*table.MemTable{})

	go db.flushWorker()

	return db
}

func (db *DB) Close() {
	db.sstables.CloseAll()
}

func (db *DB) NewTransaction() *Txn {
	return NewTxn(db)
}

// Set inserts or updates a key-value pair in the database
// and returns the value that was set.
// If the memtable exceeds its max size after the set,
// it will be flushed to disk in the background.
func (db *DB) Set(key []byte, value []byte) []byte {
	if db.memTable.ShouldFlush(key, value) {
		// Check if it's just an update (key already exists)
		_, found := db.memTable.Get(key)
		if !found {
			old := db.memTable
			db.wg.Add(1)
			db.flushJobs <- old
			db.memTable = table.NewMemTable(db.logger, old.MaxSize(), wal.NewWAL(db.folder, false))
		}
	}
	db.memTable.Set(key, value)
	return value
}

func (db *DB) getFromImm(keyBytes []byte) ([]byte, bool) {
	immMemTables := db.immMemTables.Load()
	if len(*immMemTables) > 0 {
		for _, mt := range *immMemTables {
			value, found := mt.Get(keyBytes)
			if found {
				return value, true
			}
		}
	}
	return []byte{}, false
}

// Get retrieves the value/found for a given key
func (db *DB) Get(key []byte) ([]byte, bool) {
	if value, found := db.memTable.Get(key); found {
		return value, true
	}

	if value, found := db.getFromImm(key); found {
		return value, true
	}

	if value, found := db.sstables.Search(key); found {
		return value, true
	}

	return nil, false
}

// Delete marks a key as deleted by setting its value to a tombstone (0x00)
func (db *DB) Delete(key []byte) {
	_ = db.Set(key, []byte{0x00})
	db.sstables.DeleteIndex(key)
}

// Range retrieves all key-value pairs in the specified key range [fromKey, toKey]
// and returns them as a skiplist
func (db *DB) Range(fromKey []byte, toKey []byte) *skiplist.Skiplist {
	if bytes.Compare(fromKey, toKey) > 0 {
		return skiplist.New()
	}

	result := skiplist.New()

	db.sstables.Range(result, fromKey, toKey)

	immMemTables := db.immMemTables.Load()
	if len(*immMemTables) > 0 {
		for _, mt := range *immMemTables {
			mt.Range(result, fromKey, toKey)
		}
	}

	db.memTable.Range(result, fromKey, toKey)

	return result
}

// MemTableSize returns the current size of the MemTable in bytes
func (db *DB) MemTableSize() int {
	return db.memTable.Size()
}

// NumSSTables returns the number of SSTables currently in the database
func (db *DB) NumSSTables() int {
	return db.sstables.Len()
}

// Metrics returns a json format of the database metrics
// metrics include total keys, number of SSTables, MemTable size, SSTables size
func (db *DB) Metrics() DBMetrics {
	// TODO : use atomic counters for these metrics instead
	// of locking for better performance
	db.mu.RLock()
	defer db.mu.RUnlock()

	memTableKeys := db.memTable.Len()
	sstKeys := db.sstables.TotalKeys()
	numSST := db.NumSSTables()
	memTableSize := db.MemTableSize()
	diskStorage := db.sstables.TotalSize()

	immMemTables := db.immMemTables.Load()
	pendingFlush := len(*immMemTables)

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
	compact.MaybeCompactToUpperLevel(db.sstables)
	compact.MaybeCompactL0(db.sstables)
}

// WaitJobs waits for all pending flush and compaction jobs to complete
func (db *DB) WaitJobs() {
	db.wg.Wait()
}

func (db *DB) flushWorker() {
	for mt := range db.flushJobs {
		defer db.wg.Done()

		db.appendImmMemTable(mt)
		mt.SetLoggerImmutable()

		newTable := table.NewSSTable(db.folder, 0, db.sstables.Len(), true, 0)
		_, err := newTable.Open()
		if err != nil {
			db.logger.Error("Failed to open new SSTable", "error", err)
			continue
		}

		err = mt.Flush(newTable)
		if err != nil {
			db.logger.Error("Failed to flush memtable", "error", err)
			continue
		}

		db.sstables.Add(newTable)

		db.removeImmMemTable(mt)
		db.wg.Done()
	}
}

func (db *DB) removeImmMemTable(mt *table.MemTable) {
	for {
		immMemTables := db.immMemTables.Load()
		newImmMemTables := make([]*table.MemTable, len(*immMemTables)-1)

		for _, m := range *immMemTables {
			if m != mt {
				newImmMemTables = append(newImmMemTables, m)
			}
		}

		if db.immMemTables.CompareAndSwap(immMemTables, &newImmMemTables) {
			return
		}
	}
}

func (db *DB) appendImmMemTable(mt *table.MemTable) {
	for {
		immMemTables := db.immMemTables.Load()
		newImmMemTables := append(append([]*table.MemTable{}, (*immMemTables)...), mt)
		if db.immMemTables.CompareAndSwap(immMemTables, &newImmMemTables) {
			return
		}
	}
}
