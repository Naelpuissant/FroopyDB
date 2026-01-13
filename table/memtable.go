package table

import (
	"fmt"
	"froopydb/logger"
	"froopydb/wal"
	"froopydb/x"
	"os"

	"github.com/huandu/skiplist"
)

type MemTable struct {
	logger *logger.Logger

	maxSize int //Bytes
	size    int //Bytes

	wal   *wal.WAL
	store *skiplist.SkipList
}

func NewMemTable(logger *logger.Logger, maxSize int, wal *wal.WAL) *MemTable {
	store := skiplist.New(skiplist.Bytes)
	fsize := int(wal.GetFileSize())

	memTableSize := 0
	if fsize > 0 {
		memTableSize, err := loadMemTableFromFile(store, wal.File())
		if err != nil {
			logger.Error("Failed to recover memtable from WAL", "error", err)
			panic(err)
		}
		logger.Debug("Recovered memtable from WAL", "size", memTableSize)
	}

	return &MemTable{
		maxSize: maxSize,
		size:    memTableSize,
		wal:     wal,
		store:   store,
	}
}

func loadMemTableFromFile(store *skiplist.SkipList, file *os.File) (int, error) {
	fstat, _ := file.Stat()

	fsize := fstat.Size()
	offset := int64(0)
	memTableSize := 0

	for offset < fsize {
		klenBytes := make([]byte, 2)
		file.ReadAt(klenBytes, offset)
		offset += 2

		vlenBytes := make([]byte, 2)
		file.ReadAt(vlenBytes, offset)
		offset += 2

		klen := x.BytesToUint16(klenBytes)
		key := make([]byte, klen)
		file.ReadAt(key, offset)
		offset += int64(klen)

		vlen := x.BytesToUint16(vlenBytes)
		val := make([]byte, vlen)
		file.ReadAt(val, offset)
		offset += int64(vlen)

		store.Set(key, val)
		memTableSize += int(klen) + int(vlen)
	}

	if offset != fsize {
		err := fmt.Errorf("%w: %d/%d", ErrMemTableRecoveryFailed, offset, fsize)
		return memTableSize, err
	}

	return memTableSize, nil
}

func (m *MemTable) Flush(sst *SSTable) {
	// Handle error properly here
	sst.InitWriter()
	for elem := m.store.Front(); elem != nil; elem = elem.Next() {
		err := sst.WriteDataBlock(elem.Key().([]byte), elem.Value.([]byte))
		if err != nil {
			panic(err)
		}
	}
	indexOffset, _ := sst.WriteIndex()
	sst.WriteMetadata(indexOffset)
	sst.FlushWriter()
	m.wal.Finish()
	sst.Ready()
}

func (m *MemTable) Set(key, value []byte) {
	m.store.Set(key, value)
	m.wal.Write(key, value)
	m.size += len(key) + len(value)
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	value, found := m.store.GetValue(key)
	if !found || value == nil || value.([]byte)[0] == 0x00 {
		return []byte{}, false
	}
	return value.([]byte), found
}

func (m *MemTable) ShouldFlush(key, value []byte) bool {
	return m.maxSize <= m.size+len(key)+len(value)
}

func (m *MemTable) MaxSize() int {
	return m.maxSize
}

func (m *MemTable) SetLoggerImmutable() {
	m.wal.Immutable()
}
