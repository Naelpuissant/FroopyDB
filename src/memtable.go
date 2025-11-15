package src

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/huandu/skiplist"
)

type WAL struct {
	folder string
	file   *os.File
}

func openLogFile(folder string) *os.File {
	dir, _ := os.ReadDir(folder)

	for _, entry := range dir {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			file, _ := os.OpenFile(filepath.Join(folder, entry.Name()), os.O_APPEND|os.O_RDWR, 0777)
			return file
		}
	}

	now := time.Now().UnixMilli()
	filename := fmt.Sprintf("%d.log", now)
	path := filepath.Join(folder, filename)
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)

	return file
}

func NewWAL(folder string) *WAL {
	file := openLogFile(folder)
	return &WAL{
		folder: folder,
		file:   file,
	}
}

func (wal *WAL) Write(key, value []byte) {
	klen := Uint16ToBytes(uint16(len(key)))
	vlen := Uint16ToBytes(uint16(len(value)))

	wal.file.Write(klen)
	wal.file.Write(vlen)
	wal.file.Write(key)
	wal.file.Write(value)
}

func (wal *WAL) Reset() {
	os.Remove(wal.file.Name())
	wal.file = openLogFile(wal.folder)
}

func (wal *WAL) getFileSize() int64 {
	fs, _ := wal.file.Stat()
	return fs.Size()
}

type MemTable struct {
	maxSize         int //Bytes
	size            int //Bytes
	logger          *WAL
	store           *skiplist.SkipList
	immutableStores []*skiplist.SkipList
}

func NewMemTable(maxSize int, logger *WAL) *MemTable {
	store := skiplist.New(skiplist.Bytes)
	fsize := int(logger.getFileSize())

	memTableSize := 0
	if fsize > 0 {
		memTableSize = loadMemTableFromFile(store, logger.file)
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
		klenBytes := make([]byte, 4)
		file.ReadAt(klenBytes, offset)
		offset += 4

		vlenBytes := make([]byte, 4)
		file.ReadAt(vlenBytes, offset)
		offset += 4

		klen := BytesToUint16(klenBytes)
		key := make([]byte, klen)
		file.ReadAt(key, offset)
		offset += int64(klen)

		vlen := BytesToUint16(vlenBytes)
		val := make([]byte, vlen)
		file.ReadAt(val, offset)
		offset += int64(vlen)

		store.Set(key, val)
		memTableSize += int(klen) + int(vlen)
	}
	return memTableSize
}

func (m *MemTable) Flush(seg *SStable) {
	// Aptent to start parallel sstable write
	////
	currStore := m.store
	m.immutableStores = append(m.immutableStores, currStore)
	////
	m.store = skiplist.New(skiplist.Bytes)

	for elem := currStore.Front(); elem != nil; elem = elem.Next() {
		seg.WriteBlock(([4]byte)(elem.Key().([]byte)), elem.Value.([]byte))
	}
	seg.WriteIndices()
	m.logger.Reset()
	m.size = 0
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
