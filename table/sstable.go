package table

import (
	"fmt"
	"froopydb/x"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type SSTable struct {
	name   string
	folder string
	level  int
	incr   int
	size   int
	file   *os.File
	index  map[[4]byte]uint32 // uint32 key
}

func newSSTableName(folder string, level int, incr int, tmp bool) string {
	prefix := ".sst"
	if tmp {
		prefix = ".sst.tmp"
	}
	filename := fmt.Sprintf("%d_%d%s", level, incr, prefix)
	return filepath.Join(folder, filename)
}

func NewSSTable(folder string, level int, incr int, tmp bool, size int) *SSTable {
	name := newSSTableName(folder, level, incr, tmp)
	return &SSTable{
		name:   name,
		folder: folder,
		level:  level,
		incr:   incr,
		size:   size,
		index:  map[[4]byte]uint32{},
	}
}

// ParseSSTableName parses "<folder>/<level>_<incr>.sst"
func parseSSTableName(path string) (folder string, level int, incr int) {
	folder = filepath.Dir(path)
	base := filepath.Base(path) // e.g. "0_12.sst"

	// Remove extension
	name := strings.TrimSuffix(base, filepath.Ext(base)) // "0_12"

	parts := strings.Split(name, "_")

	level, _ = strconv.Atoi(parts[0])
	incr, _ = strconv.Atoi(parts[1])

	return folder, level, incr
}

func NewSSTableFromFile(file *os.File) *SSTable {
	fstat, _ := file.Stat()
	end := fstat.Size()

	endOffset := end - 4

	startOffsetBytes := make([]byte, 4)
	file.ReadAt(startOffsetBytes, endOffset)

	startOffset := int64(x.BytesToUint32(startOffsetBytes))
	indexBlockSize := startOffset

	index := map[[4]byte]uint32{}
	for startOffset < endOffset {
		klenBytes := make([]byte, 2)
		file.ReadAt(klenBytes, startOffset)
		startOffset += 2

		klen := x.BytesToUint16(klenBytes)
		key := make([]byte, klen)
		file.ReadAt(key, startOffset)
		startOffset += int64(klen)

		offset := make([]byte, 4)
		file.ReadAt(offset, startOffset)
		startOffset += 4

		index[[4]byte(key)] = x.BytesToUint32(offset)
	}

	if startOffset != endOffset {
		panic("Failed to recover sstable")
	}

	println(file.Name() + " : sstable recovered")

	folder, level, incr := parseSSTableName(file.Name())
	return &SSTable{
		size:   int(indexBlockSize),
		folder: folder,
		level:  level,
		incr:   incr,
		name:   file.Name(),
		index:  index,
		file:   file,
	}
}

func (sst *SSTable) WriteBlock(key [4]byte, value []byte) {
	offset, _ := sst.file.Seek(0, io.SeekCurrent)

	vlen := x.Uint16ToBytes(uint16(len(value)))

	sst.file.Write(vlen)
	sst.file.Write(value)

	if len(value) != 0 && value[0] != 0x00 {
		sst.index[key] = uint32(offset)
	}

	sst.size += 16 + len(value)
}

func (sst *SSTable) WriteIndices() {
	indexOffset, _ := sst.file.Seek(0, io.SeekCurrent)
	for key, offset := range sst.index {
		klen := x.Uint16ToBytes(uint16(len(key)))
		sst.file.Write(klen)
		sst.file.Write(key[:])
		sst.file.Write(x.Uint32ToBytes(offset))
		sst.size += 16 + len(key) + 32
	}
	sst.file.Write(x.Uint32ToBytes(uint32(indexOffset)))
	sst.size += 32
}

func (sst *SSTable) Open() *os.File {
	file, _ := os.OpenFile(sst.name, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	sst.file = file
	return file
}

func (sst *SSTable) Close() {
	sst.file.Close()
}

func (sst *SSTable) Remove() {
	sst.Close()
	os.Remove(sst.name)
}

func (sst *SSTable) Rename(new string) {
	os.Rename(sst.name, new)
	sst.name = new
}

func (sst *SSTable) DeleteIndex(key [4]byte) {
	delete(sst.index, key)
}

func (sst *SSTable) Search(key [4]byte) []byte {
	offset, found := sst.index[key]
	if !found {
		return []byte{}
	}
	vlen := make([]byte, 2)
	sst.file.ReadAt(vlen, int64(offset))

	value := make([]byte, x.BytesToUint16(vlen))
	sst.file.ReadAt(value, int64(offset)+2)

	return value
}

func (sst *SSTable) Index() map[[4]byte]uint32 {
	return sst.index
}

func (sst *SSTable) Ready() {
	old := sst.name
	sst.name = strings.TrimSuffix(sst.name, ".tmp")
	os.Rename(old, sst.name)
	sst.file.Sync()
}

func (sst *SSTable) GetMinMax() (int, int) {
	if len(sst.index) == 0 {
		return 0, 0
	}

	var minKey uint32 = ^uint32(0) // max possible uint32
	var maxKey uint32 = 0

	for keyBytes := range sst.index {
		key := x.BytesToUint32(keyBytes[:])

		if key < minKey {
			minKey = key
		}
		if key > maxKey {
			maxKey = key
		}
	}

	return int(minKey), int(maxKey)
}

func (sst *SSTable) Folder() string {
	return sst.folder
}

func (sst *SSTable) Incr() int {
	return sst.incr
}

func (sst *SSTable) Name() string {
	return sst.name
}

func (sst *SSTable) ResetFilePointer() {
	sst.file.Seek(0, io.SeekStart)
}

type SSTableStore struct {
	tables         map[int][]*SSTable
	maxLevel       int
	folder         string
	sstableMaxSize int
	mu             sync.Mutex
}

func NewSSTableStore(folder string, sstableMaxSize int) *SSTableStore {
	maxLevel := 1
	tables := map[int][]*SSTable{}
	for i := range maxLevel {
		tables[i] = []*SSTable{}
	}

	loadSSTablesFromFile(tables, folder)

	return &SSTableStore{
		tables:         tables,
		maxLevel:       1,
		folder:         folder,
		sstableMaxSize: sstableMaxSize,
	}
}

func loadSSTablesFromFile(tables map[int][]*SSTable, folder string) {
	dir, _ := os.ReadDir(folder)

	for _, entry := range dir {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
			file, _ := os.OpenFile(filepath.Join(folder, entry.Name()), os.O_RDONLY, 0777)
			table := NewSSTableFromFile(file)
			tables[table.level] = append(tables[table.level], table)
		}
	}
}

func (store *SSTableStore) Add(sst *SSTable) *SSTable {
	store.tables[sst.level] = append(store.tables[sst.level], sst)
	return sst
}

func (store *SSTableStore) AddNew() *SSTable {
	table := NewSSTable(store.folder, 0, store.Len(), false, 0)
	store.tables[0] = append(store.tables[0], table)
	return table
}

func (store *SSTableStore) Len() int {
	n := 0
	for _, level := range store.tables {
		n += len(level)
	}
	return n
}

func (store *SSTableStore) CloseAll() {
	for _, level := range store.tables {
		for _, table := range level {
			table.Close()
		}
	}
}

func (store *SSTableStore) Remove(sst *SSTable) {
	store.mu.Lock()
	defer store.mu.Unlock()

	level, found := store.tables[sst.level]
	if found {
		for i, table := range level {
			if table.name == sst.name {
				store.tables[sst.level] = append(store.tables[sst.level][:i], store.tables[sst.level][i+1:]...)
				sst.Remove()
				return
			}
		}
	}
}

func (store *SSTableStore) Replace(old, new *SSTable) {
	store.Remove(old)

	store.mu.Lock()
	defer store.mu.Unlock()

	store.tables[new.level] = append(store.tables[new.level], new)
	sort.Slice(store.tables[new.level], func(a, b int) bool {
		return store.tables[new.level][a].name < store.tables[new.level][b].name
	})
}

func (store *SSTableStore) Search(key [4]byte) []byte {
	for _, level := range store.tables {
		for i := len(level) - 1; i >= 0; i-- {
			value := level[i].Search(key)
			if len(value) != 0 {
				return value
			}
		}
	}
	return []byte{}
}

func (store *SSTableStore) DeleteIndex(key [4]byte) {
	for _, level := range store.tables {
		for _, table := range level {
			table.DeleteIndex(key)
		}
	}
}

func (store *SSTableStore) Tables() map[int][]*SSTable {
	return store.tables
}
