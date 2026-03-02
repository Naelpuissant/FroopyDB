package froopydb

import (
	"bytes"
	"froopydb/compact"
	"froopydb/logger"
	"froopydb/skiplist"
	"froopydb/table"
	"froopydb/wal"
	"os"
	"sync"
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

	immMu        sync.Mutex
	immMemTables []*table.MemTable
	flushJobs    chan *table.MemTable
	wg           sync.WaitGroup
}

func NewDB(config *DBConfig) *DB {
	if config.ClearOnStart {
		os.RemoveAll(config.Folder)
	}

	logger := logger.NewLogger(config.LogLevel)

	os.MkdirAll(config.Folder, 0777)

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

func (db *DB) NewTransaction() *Txn {
	return NewTxn(db)
}

// Set inserts or updates a key-value pair in the database.
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

// Get retrieves the value for a given key.
func (db *DB) Get(key []byte) []byte {
	value, found := db.memTable.Get(key)
	if found {
		return value
	}

	value, found = db.getFromImm(key)
	if found {
		return value
	}

	value, err := db.sstables.Search(key)
	if err != nil {
		println(err)
	}

	return value
}

// Delete marks a key as deleted by setting its value to a tombstone (0x00).
func (db *DB) Delete(key []byte) []byte {
	line := db.Set(key, []byte{0x00})
	db.sstables.DeleteIndex(key)
	return line
}

// Range retrieves all key-value pairs in the specified key range [fromKey, toKey]
// and returns them as a skiplist.
func (db *DB) Range(fromKey []byte, toKey []byte) *skiplist.Skiplist {
	if bytes.Compare(fromKey, toKey) > 0 {
		return skiplist.New()
	}

	result := skiplist.New()

	db.sstables.Range(result, fromKey, toKey)
	if len(db.immMemTables) > 0 {
		for i := len(db.immMemTables) - 1; i >= 0; i-- {
			db.immMemTables[i].Range(result, fromKey, toKey)
		}
	}

	db.memTable.Range(result, fromKey, toKey)

	return result
}

// Metrics returns a json format of the database metrics
// metrics include total keys, number of SSTables, MemTable size, SSTables size
func (db *DB) Metrics() DBMetrics {
	memTableKeys := db.memTable.Len()
	sstKeys := db.sstables.TotalKeys()
	numSST := db.sstables.Len()
	memTableSize := db.memTable.Size()
	diskStorage := db.sstables.TotalSize()
	pendingFlush := len(db.immMemTables)

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

// WaitFlush waits for all pending memtable flushes goroutines to complete.
func (db *DB) WaitFlush() {
	db.wg.Wait()
}

func (db *DB) flushWorker() {
	for mt := range db.flushJobs {
		db.immMemTables = append(db.immMemTables, mt)
		mt.SetLoggerImmutable()

		newTable := table.NewSSTable(db.folder, 0, db.sstables.Len(), true, 0)
		_, err := newTable.Open()
		if err != nil {
			db.logger.Error("Failed to open new SSTable", "error", err)
		}

		err = mt.Flush(newTable)
		if err != nil {
			db.logger.Error("Failed to flush memtable", "error", err)
		}

		db.sstables.Add(newTable)

		db.removeImmMemTable(mt)
		db.wg.Done()
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
