package src

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/huandu/skiplist"
)

type WAL struct {
	folder  string
	file    *os.File
	writeCh chan []byte
}

func openLogFile(folder string, tryRecover bool) *os.File {
	if tryRecover {
		dir, _ := os.ReadDir(folder)

		for _, entry := range dir {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
				file, _ := os.OpenFile(filepath.Join(folder, entry.Name()), os.O_APPEND|os.O_RDWR, 0777)
				return file
			}
		}
	}

	now := time.Now().UnixMilli()
	filename := fmt.Sprintf("%d_%d.log", now, rand.Intn(10000))
	path := filepath.Join(folder, filename)
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)

	return file
}

func NewWAL(folder string, tryRecover bool) *WAL {
	file := openLogFile(folder, tryRecover)
	wal := &WAL{
		folder:  folder,
		file:    file,
		writeCh: make(chan []byte),
	}
	go wal.writer()
	return wal
}

func (wal *WAL) writer() {
	for record := range wal.writeCh {
		wal.file.Write(record)
	}
}

func (wal *WAL) Write(key, value []byte) {
	klen := Uint16ToBytes(uint16(len(key)))
	vlen := Uint16ToBytes(uint16(len(value)))

	var buf bytes.Buffer
	buf.Write(klen)
	buf.Write(vlen)
	buf.Write(key)
	buf.Write(value)

	wal.writeCh <- buf.Bytes()
}

// Close and remove log file
func (wal *WAL) Finish() {
	wal.file.Close()
	os.Remove(wal.file.Name() + ".imm")
}

// Mark log file as immutable by adding .imm prefix
func (wal *WAL) Immutable() {
	os.Rename(wal.file.Name(), wal.file.Name()+".imm")
}

func (wal *WAL) getFileSize() int64 {
	fs, _ := wal.file.Stat()
	return fs.Size()
}

type MemTable struct {
	maxSize int //Bytes
	size    int //Bytes
	logger  *WAL
	store   *skiplist.SkipList
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
		klenBytes := make([]byte, 2)
		file.ReadAt(klenBytes, offset)
		offset += 2

		vlenBytes := make([]byte, 2)
		file.ReadAt(vlenBytes, offset)
		offset += 2

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

	if offset != fsize {
		panic(fmt.Sprintf("Failed to recover memtable: %d/%d\n", offset, fsize))
	}

	println(file.Name() + " : memtable recovered")
	return memTableSize
}

func (m *MemTable) Flush(sst *SSTable) {
	for elem := m.store.Front(); elem != nil; elem = elem.Next() {
		sst.WriteBlock(([4]byte)(elem.Key().([]byte)), elem.Value.([]byte))
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
