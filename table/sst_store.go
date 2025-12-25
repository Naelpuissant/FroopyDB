package table

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

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

	err := loadSSTablesFromFile(tables, folder)
	if err != nil {
		panic(fmt.Errorf("failed to load SSTables from %s: %v", folder, err))
	}

	return &SSTableStore{
		tables:         tables,
		maxLevel:       1,
		folder:         folder,
		sstableMaxSize: sstableMaxSize,
	}
}

func loadSSTablesFromFile(tables map[int][]*SSTable, folder string) error {
	dir, _ := os.ReadDir(folder)

	for _, entry := range dir {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
			file, _ := os.OpenFile(filepath.Join(folder, entry.Name()), os.O_RDONLY, 0777)
			table, err := NewSSTableFromFile(file)
			if err != nil {
				return err
			}
			tables[table.level] = append(tables[table.level], table)
		}
	}
	return nil
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
			value, err := level[i].Search(key)
			if err != nil {
				panic(err) // TODO: handle error properly
			}
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
