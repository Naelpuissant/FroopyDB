package src

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type SStable struct {
	name  string
	size  int
	file  *os.File
	index map[[4]byte]uint32 // uint32 key
}

func newSSTableName(folder string, incr int) string {
	filename := fmt.Sprintf("%d.sst", incr)
	return filepath.Join(folder, filename)
}

func NewSSTable(name string, size int) *SStable {
	return &SStable{
		name:  name,
		size:  size,
		index: map[[4]byte]uint32{},
	}
}

func (sst *SStable) WriteBlock(key [4]byte, value []byte) {
	offset, _ := sst.file.Seek(0, io.SeekCurrent)

	vlen := Uint16ToBytes(uint16(len(value)))

	sst.file.Write(vlen)
	sst.file.Write(value)

	if len(value) != 0 && value[0] != 0x00 {
		sst.index[key] = uint32(offset)
	}

	sst.size += 16 + len(value)
}

func (sst *SStable) WriteIndices() {
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

func (sst *SStable) Open() *os.File {
	file, _ := os.OpenFile(sst.name, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	sst.file = file
	return file
}

func (sst *SStable) Close() {
	sst.file.Close()
}

func (sst *SStable) Remove() {
	sst.Close()
	os.Remove(sst.name)
}

func (sst *SStable) Rename(new string) {
	os.Rename(sst.name, new)
	sst.name = new
}

func (sst *SStable) DeleteIndex(key [4]byte) {
	delete(sst.index, key)
}

func (sst *SStable) Search(key [4]byte) []byte {
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

type SSTableStore struct {
	tables         []*SStable // Map maybe better
	folder         string
	sstableMaxSize int
}

func NewSSTableStore(folder string, sstableMaxSize int) *SSTableStore {
	tables := []*SStable{}
	return &SSTableStore{
		tables:         tables,
		folder:         folder,
		sstableMaxSize: sstableMaxSize,
	}
}

func (store *SSTableStore) AddNew() *SStable {
	name := newSSTableName(store.folder, len(store.tables))
	table := NewSSTable(name, 0)
	store.tables = append(store.tables, table)
	return table
}

func (store *SSTableStore) CloseAll() {
	for _, table := range store.tables {
		table.Close()
	}
}

func (store *SSTableStore) Last() *SStable {
	return store.tables[len(store.tables)-1]
}

func (store *SSTableStore) Remove(sst *SStable) {
	for i := len(store.tables) - 1; i >= 0; i-- {
		if store.tables[i].name == sst.name {
			store.tables = append(store.tables[:i], store.tables[i+1:]...)
			sst.Remove()
			return
		}
	}
}

func (store *SSTableStore) Replace(old, new *SStable) {
	for i := len(store.tables) - 1; i >= 0; i-- {
		if store.tables[i].name == old.name {
			old.Remove()
			store.tables[i] = new

			// Keep it sorted... meh
			sort.Slice(store.tables, func(a, b int) bool {
				return store.tables[a].name < store.tables[b].name
			})
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
