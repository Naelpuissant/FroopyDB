package table

import (
	"fmt"
	"froopydb/logger"
	"froopydb/skiplist"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type SSTableStore struct {
	logger *logger.Logger

	tables   map[int][]*SSTable
	maxLevel int
	folder   string
	mu       sync.Mutex
}

func NewSSTableStore(logger *logger.Logger, folder string) (*SSTableStore, error) {
	maxLevel := 1
	tables := map[int][]*SSTable{}
	for i := range maxLevel {
		tables[i] = []*SSTable{}
	}

	// TODO : limit logger passed to SSTable ?
	err := loadSSTablesFromFile(logger, tables, folder)
	if err != nil {
		return nil, fmt.Errorf("failed to load SSTables from %s: %w", folder, err)
	}
	logger.Debug("Loaded SSTables from folder", "folder", folder, "tables", len(tables))

	return &SSTableStore{
		logger:   logger,
		tables:   tables,
		maxLevel: 1,
		folder:   folder,
	}, nil
}

func loadSSTablesFromFile(logger *logger.Logger, tables map[int][]*SSTable, folder string) error {
	dir, err := os.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, entry := range dir {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
			file, err := os.OpenFile(filepath.Join(folder, entry.Name()), os.O_RDONLY, 0444)
			if err != nil {
				return err
			}
			table, err := NewSSTableFromFile(file)
			if err != nil {
				return err
			}
			logger.Debug("Recovered SSTable index", "size", len(table.index))
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

func (store *SSTableStore) CloseAll() error {
	for _, level := range store.tables {
		for _, table := range level {
			err := table.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
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

func (store *SSTableStore) Search(key []byte) ([]byte, error) {
	for _, level := range store.tables {
		for i := len(level) - 1; i >= 0; i-- {
			value, err := level[i].Search(key)
			if err != nil {
				return []byte{}, err
			}
			if len(value) != 0 {
				return value, nil
			}
		}
	}
	return []byte{}, nil
}

func (store *SSTableStore) Range(res *skiplist.Skiplist, fromKey, toKey []byte) {
	for _, level := range store.tables {
		for i := len(level) - 1; i >= 0; i-- {
			level[i].Range(res, fromKey, toKey)
		}
	}
}

func (store *SSTableStore) DeleteIndex(key []byte) {
	for _, level := range store.tables {
		for _, table := range level {
			table.DeleteIndex(key)
		}
	}
}

func (store *SSTableStore) Tables() map[int][]*SSTable {
	return store.tables
}

func (store *SSTableStore) TotalKeys() int {
	n := 0
	for _, level := range store.tables {
		for _, table := range level {
			n += table.Len()
		}
	}
	return n
}

func (store *SSTableStore) TotalSize() int {
	n := 0
	for _, level := range store.tables {
		for _, table := range level {
			n += table.size
		}
	}
	return n
}
