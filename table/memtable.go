package table

import (
	"fmt"
	"os"

	"froopydb/logger"
	"froopydb/wal"
	"froopydb/x"

	"froopydb/skiplist"
)

type MemTable struct {
	logger *logger.Logger

	maxSize int // Bytes

	wal   *wal.WAL
	store *skiplist.Skiplist
}

func NewMemTable(logger *logger.Logger, maxSize int, wal *wal.WAL) *MemTable {
	store := skiplist.New()
	fsize := int(wal.GetFileSize())

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
		wal:     wal,
		store:   store,
	}
}

func loadMemTableFromFile(store *skiplist.Skiplist, file *os.File) (int, error) {
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

		store.Insert(key, val)
		memTableSize += int(klen) + int(vlen)
	}

	if offset != fsize {
		err := fmt.Errorf("%w: %d/%d", ErrMemTableRecoveryFailed, offset, fsize)
		return memTableSize, err
	}

	return memTableSize, nil
}

func (m *MemTable) Flush(sst *SSTable) error {
	// Handle error properly here
	sst.InitWriter()
	for elem := m.store.First(); elem != nil; elem = elem.Next() {
		err := sst.WriteDataBlock(elem.Key, elem.Value)
		if err != nil {
			return err
		}
	}

	indexOffset, err := sst.WriteIndex()
	if err != nil {
		return err
	}

	err = sst.WriteMetadata(indexOffset)
	if err != nil {
		return err
	}

	err = sst.FlushWriter()
	if err != nil {
		return err
	}

	m.wal.Finish()
	err = sst.Ready()
	if err != nil {
		return err
	}
	return nil
}

func (m *MemTable) Set(key, value []byte) {
	m.store.Insert(key, value)
	m.wal.Write(key, value)
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	node, found := m.store.Search(key)
	if !found || node.IsDeleted() {
		return []byte{}, false
	}
	return node.Value, true
}

// Range get range of keys from fromKey to toKey (inclusive)
func (m *MemTable) Range(res map[string][]byte, fromKey []byte, toKey []byte) {
	nodes := m.store.Range(fromKey, toKey)
	for _, node := range nodes {
		if !node.IsDeleted() {
			res[string(node.Key)] = node.Value
		} else {
			delete(res, string(node.Key))
		}
	}
}

func (m *MemTable) ShouldFlush(key, value []byte) bool {
	return m.maxSize <= int(m.store.Size())+len(key)+len(value)
}

func (m *MemTable) MaxSize() int {
	return m.maxSize
}

func (m *MemTable) SetLoggerImmutable() {
	m.wal.Immutable()
}

func (m *MemTable) Len() int {
	return int(m.store.Length())
}

func (m *MemTable) Size() int {
	return int(m.store.Size())
}

func (m *MemTable) WAL() *wal.WAL {
	return m.wal
}
