package src

import (
	"fmt"
	"os"
	"time"

	"github.com/huandu/skiplist"
)

type Log struct {
	folder string
	file   *os.File
}

func openLogFile(folder string) *os.File {
	now := time.Now().UnixMilli()
	filename := fmt.Sprintf("%s/%d.log", folder, now)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	return file
}

func NewLog(folder string) *Log {
	file := openLogFile(folder)
	return &Log{
		folder: folder,
		file:   file,
	}
}

func (l *Log) Write(key, value []byte) {
	klen := Uint16ToBytes(uint16(len(key)))
	vlen := Uint16ToBytes(uint16(len(value)))

	l.file.Write(klen)
	l.file.Write(key)
	l.file.Write(vlen)
	l.file.Write(value)
}

func (l *Log) Reset() {
	os.Remove(l.file.Name())
	l.file = openLogFile(l.folder)
}

type MemTable struct {
	maxSize         int //Bytes
	size            int //Bytes
	logger          *Log
	store           *skiplist.SkipList
	immutableStores []*skiplist.SkipList
}

func NewMemTable(maxSize int, logger *Log) *MemTable {
	store := skiplist.New(skiplist.Bytes)
	return &MemTable{
		maxSize: maxSize,
		size:    0,
		logger:  logger,
		store:   store,
	}
}

func (m *MemTable) Flush(seg *Segment) {
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
