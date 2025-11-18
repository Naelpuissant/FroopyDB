package src

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

	startOffset := int64(BytesToUint32(startOffsetBytes))
	indexBlockSize := startOffset

	index := map[[4]byte]uint32{}
	for startOffset < endOffset {
		klenBytes := make([]byte, 2)
		file.ReadAt(klenBytes, startOffset)
		startOffset += 2

		klen := BytesToUint16(klenBytes)
		key := make([]byte, klen)
		file.ReadAt(key, startOffset)
		startOffset += int64(klen)

		offset := make([]byte, 4)
		file.ReadAt(offset, startOffset)
		startOffset += 4

		index[[4]byte(key)] = BytesToUint32(offset)
	}

	if startOffset != endOffset {
		panic("Failed to recover sstable")
	}

	println(file.Name() + " : sstable recovered")

	folder, level, incr := parseSSTableName(file.Name())
	return &SSTable{
		size:   int(indexBlockSize),
		folder: folder, // todo : fix this
		level:  level,
		incr:   incr,
		name:   file.Name(),
		index:  index,
		file:   file,
	}
}

func (sst *SSTable) WriteBlock(key [4]byte, value []byte) {
	offset, _ := sst.file.Seek(0, io.SeekCurrent)

	vlen := Uint16ToBytes(uint16(len(value)))

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
		klen := Uint16ToBytes(uint16(len(key)))
		sst.file.Write(klen)
		sst.file.Write(key[:])
		sst.file.Write(Uint32ToBytes(offset))
		sst.size += 16 + len(key) + 32
	}
	sst.file.Write(Uint32ToBytes(uint32(indexOffset)))
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

	value := make([]byte, BytesToUint16(vlen))
	sst.file.ReadAt(value, int64(offset)+2)

	return value
}

func (sst *SSTable) Index() map[[4]byte]uint32 {
	return sst.index
}

type SSTableStore struct {
	tables         []*SSTable // Map maybe better
	folder         string
	sstableMaxSize int
}

func NewSSTableStore(folder string, sstableMaxSize int) *SSTableStore {
	tables := []*SSTable{}
	loadSSTablesFromFile(tables, folder)
	return &SSTableStore{
		tables:         tables,
		folder:         folder,
		sstableMaxSize: sstableMaxSize,
	}
}

func loadSSTablesFromFile(tables []*SSTable, folder string) {
	dir, _ := os.ReadDir(folder)

	for _, entry := range dir {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
			file, _ := os.OpenFile(filepath.Join(folder, entry.Name()), os.O_RDONLY, 0777)
			tables = append(tables, NewSSTableFromFile(file))
		}
	}
}

func (store *SSTableStore) AddNew() *SSTable {
	table := NewSSTable(store.folder, 0, len(store.tables), false, 0)
	store.tables = append(store.tables, table)
	return table
}

func (store *SSTableStore) CloseAll() {
	for _, table := range store.tables {
		table.Close()
	}
}

func (store *SSTableStore) remove(sst *SSTable) {
	for i := len(store.tables) - 1; i >= 0; i-- {
		if store.tables[i].name == sst.name {
			store.tables = append(store.tables[:i], store.tables[i+1:]...)
			sst.Remove()
			return
		}
	}
}

func (store *SSTableStore) replace(old, new *SSTable) {
	for i := len(store.tables) - 1; i >= 0; i-- {
		if store.tables[i].name == old.name {
			old.Remove()
			store.tables[i] = new
			// Keep it sorted... meh
			// sort.Slice(store.tables, func(a, b int) bool {
			// 	return store.tables[a].name < store.tables[b].name
			// })
			return
		}
	}
}

func (store *SSTableStore) Search(key [4]byte) []byte {
	for i := len(store.tables) - 1; i >= 0; i-- {
		value := store.tables[i].Search(key)
		if len(value) != 0 {
			return value
		}
	}
	return []byte{}
}

func (store *SSTableStore) DeleteIndex(key [4]byte) {
	for _, table := range store.tables {
		table.DeleteIndex(key)
	}
}

func (store *SSTableStore) Tables() []*SSTable {
	return store.tables
}

func (store *SSTableStore) MaybeCompactL0() {
	threshold := 3

	tablesToCompact := []*SSTable{}
	tablesToDelete := []*SSTable{}
	tablesToReplace := [][2]*SSTable{}

	count := 0
	for _, table := range store.tables {
		if table.level == 0 {
			count++
			if count >= threshold {
				newTable := Compact(append(tablesToCompact, table), table)
				tablesToDelete = append(tablesToDelete, tablesToCompact...)
				tablesToReplace = append(tablesToReplace, [2]*SSTable{table, newTable})
				count = 0
				tablesToCompact = []*SSTable{}
			} else {
				tablesToCompact = append(tablesToCompact, table)
			}
		}
	}

	// mut on tables here
	for _, table := range tablesToDelete {
		store.remove(table)
	}

	for _, table := range tablesToReplace {
		store.replace(table[0], table[1])
		os.Rename(table[1].name, strings.TrimSuffix(table[1].name, ".tmp"))
	}
	// release mut
}
