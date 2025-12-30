package table

import (
	"fmt"
	"froopydb/wal"
	"froopydb/x"
	"os"

	"github.com/huandu/skiplist"
)

type MemTable struct {
	maxSize int //Bytes
	size    int //Bytes
	logger  *wal.WAL
	store   *skiplist.SkipList
}

func NewMemTable(maxSize int, logger *wal.WAL) *MemTable {
	store := skiplist.New(skiplist.Bytes)
	fsize := int(logger.GetFileSize())

	memTableSize := 0
	if fsize > 0 {
		memTableSize = loadMemTableFromFile(store, logger.File())
	}

	return &MemTable{
		maxSize: maxSize,
		size:    memTableSize,
		logger:  logger,
		store:   store,
	}
}

func loadMemTableFromFile(store *skiplist.SkipList, file *os.File) int {
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
		panic(fmt.Sprintf("Failed to recover memtable: %d/%d\n", offset, fsize))
	}

	println(file.Name() + " : memtable recovered")
	return memTableSize
}

func (m *MemTable) Flush(sst *SSTable) {
	for elem := m.store.Front(); elem != nil; elem = elem.Next() {
		err := sst.WriteBlock(([4]byte)(elem.Key().([]byte)), elem.Value.([]byte))
		if err != nil {
			panic(err)
		}
	}
	sst.WriteIndices()
	m.logger.Finish()
	sst.Ready()
}

func (m *MemTable) Set(key, value []byte) {
	m.store.Set(key, value)
	m.logger.Write(key, value)
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
	m.logger.Immutable()
}
