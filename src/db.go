package src

import (
	"os"
)

var (
	KB = 1024
	MB = 1024 * KB
)

type DB struct {
	folder   string
	memTable *MemTable
	sstables *SSTableStore
}

func NewDB(folder string, sstableMaxSize int, memTableMaxSize int, clearOnStart bool) *DB {
	if sstableMaxSize == 0 {
		sstableMaxSize = 1000
	}

	if memTableMaxSize == 0 {
		memTableMaxSize = 64 * MB
	}

	// TODO : for now I'll remove db everytime but
	// I might handle the db setup from existing folder
	// aka find the latest sstable at start
	if clearOnStart {
		os.RemoveAll(folder)
	}
	os.MkdirAll(folder, 0777)

	logger := NewWAL(folder)

	memTable := NewMemTable(
		memTableMaxSize,
		logger,
	)

	sstables := NewSSTableStore(folder, sstableMaxSize)

	return &DB{
		folder:   folder,
		memTable: memTable,
		sstables: sstables,
	}
}

func (db *DB) Close() {
	db.sstables.CloseAll()
}

func (db *DB) Set(key int, value string) string {
	keyBytes := Uint32ToBytes(uint32(key))
	valueBytes := StrToBytes(value)

	if db.memTable.ShouldFlush(keyBytes, valueBytes) {
		newTable := db.sstables.AddNew()
		newTable.Open()
		db.memTable.Flush(newTable)
		db.sstables.MaybeCompactL0()

	}
	db.memTable.Set(keyBytes, valueBytes)
	return value
}

func (db *DB) Get(key int) string {
	keyBytes := Uint32ToBytes(uint32(key))
	value, found := db.memTable.Get(keyBytes)
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
