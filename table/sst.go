package table

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	// Data
	VLEN_SIZE = 2

	// Index
	KLEN_SIZE   = 2
	OFFSET_SIZE = 4

	//Metadata
	LEVEL_SIZE      = 2
	INCR_SIZE       = 2
	IDX_OFFSET_SIZE = 4
	METADATA_SIZE   = LEVEL_SIZE + INCR_SIZE + IDX_OFFSET_SIZE
)

type SSTMetadata struct {
	Level     uint16
	Incr      uint16
	IdxOffset uint32
}

func newSSTableName(folder string, level int, incr int, tmp bool) string {
	prefix := ".sst"
	if tmp {
		prefix = ".sst.tmp"
	}
	filename := fmt.Sprintf("%d_%d%s", level, incr, prefix)
	return filepath.Join(folder, filename)
}

type SSTable struct {
	name   string
	folder string
	level  int
	incr   int
	size   int

	minKey string
	maxKey string

	file   *os.File
	writer *SSTWriter
	reader *SSTReader

	index map[string]uint32
	keys  []string // Sorted index keys
}

func NewSSTable(folder string, level int, incr int, tmp bool, size int) *SSTable {
	name := newSSTableName(folder, level, incr, tmp)
	return &SSTable{
		name:   name,
		folder: folder,
		level:  level,
		incr:   incr,
		size:   size,
		index:  map[string]uint32{},
		keys:   []string{},
	}
}

func NewSSTableFromFile(file *os.File) (*SSTable, error) {
	sstReader, err := NewSSTReader(file)
	if err != nil {
		return nil, fmt.Errorf("%w : %w", ErrSSTableIndexRecoveryFailed, err)
	}

	index := map[string]uint32{}
	keys := []string{}
	minKey := ""
	maxKey := ""
	for item, err := range sstReader.Index() {
		if err != nil {
			return nil, fmt.Errorf("%w : %w", ErrSSTableIndexRecoveryFailed, err)
		}
		key := string(item.Key)
		index[key] = item.Offset
		keys = append(keys, key)
		if minKey == "" || key < minKey {
			minKey = key
		}
		if maxKey == "" || key > maxKey {
			maxKey = key
		}
	}

	filename := file.Name()
	return &SSTable{
		size:   int(sstReader.Metadata.IdxOffset),
		folder: filepath.Dir(filename),
		level:  int(sstReader.Metadata.Level),
		incr:   int(sstReader.Metadata.Incr),
		name:   filename,
		minKey: minKey,
		maxKey: maxKey,
		index:  index,
		keys:   keys,
		file:   file,
		reader: sstReader,
	}, nil
}

func (sst *SSTable) InitWriter() {
	sst.writer = NewSSTWriter(sst.file)
}

func (sst *SSTable) setMinMaxKeys(key []byte) {
	keyStr := string(key)
	if sst.minKey == "" || keyStr < sst.minKey {
		sst.minKey = keyStr
	}
	if sst.maxKey == "" || keyStr > sst.maxKey {
		sst.maxKey = keyStr
	}
}

func (sst *SSTable) WriteDataBlock(key, value []byte) error {
	offset := uint32(sst.writer.Pos)
	sst.setMinMaxKeys(key)

	err := sst.writer.WriteDataBlock(value)
	if err != nil {
		return err
	}

	// Tombstone check
	if len(value) != 0 && value[0] != 0x00 {
		sst.index[string(key)] = offset
		sst.keys = append(sst.keys, string(key))
	}

	return nil
}

// WriteIndex writes the index map to the SSTable and returns the offset where it was written.
func (sst *SSTable) WriteIndex() (uint32, error) {
	indexOffset := uint32(sst.writer.Pos)
	err := sst.writer.WriteIndex(sst.index)
	if err != nil {
		return 0, err
	}
	return indexOffset, nil
}

func (sst *SSTable) WriteMetadata(indexOffset uint32) error {
	return sst.writer.WriteMetadata(uint16(sst.level), uint16(sst.incr), indexOffset)
}

func (sst *SSTable) FlushWriter() error {
	return sst.writer.Flush()
}

func (sst *SSTable) Open() (*os.File, error) {
	file, err := os.OpenFile(sst.name, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	sst.file = file
	return file, nil
}

func (sst *SSTable) Close() error {
	return sst.file.Close()
}

func (sst *SSTable) Remove() {
	sst.Close()
	os.Remove(sst.name)
}

func (sst *SSTable) Rename(new string) {
	os.Rename(sst.name, new)
	sst.name = new
}

func (sst *SSTable) DeleteIndex(key []byte) {
	delete(sst.index, string(key))
	for i, k := range sst.keys {
		if k == string(key) {
			sst.keys = append(sst.keys[:i], sst.keys[i+1:]...)
			break
		}
	}
}

func (sst *SSTable) Search(key []byte) ([]byte, bool) {
	offset, found := sst.index[string(key)]
	if !found {
		return []byte{}, false
	}

	value, err := sst.reader.ReadValueAtOffset(int64(offset))
	if err != nil {
		// TODO : log error properly here
		return []byte{}, false
	}

	return value, true
}

func (sst *SSTable) Range(res map[string][]byte, fromKey, toKey []byte) {
	if sst.minKey > string(toKey) || sst.maxKey < string(fromKey) {
		return
	}

	for _, key := range sst.keys {
		if key < string(fromKey) {
			continue
		}
		if key > string(toKey) {
			return
		}

		value, found := sst.Search([]byte(key))
		if found {
			res[key] = value
		} else {
			delete(res, key)
		}
	}
}

func (sst *SSTable) Ready() error {
	oldName := sst.name

	sst.name = strings.TrimSuffix(sst.name, ".tmp")

	if err := os.Rename(oldName, sst.name); err != nil {
		return err
	}

	if err := sst.file.Sync(); err != nil {
		return err
	}

	sst.size = int(sst.writer.Pos)

	err := sst.setReadOnly()
	if err != nil {
		return err
	}

	sst.reader, err = NewSSTReader(sst.file)
	return err
}

func (sst *SSTable) setReadOnly() error {
	sst.writer = nil
	file, err := os.OpenFile(sst.name, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}
	wFile := sst.file
	sst.file = file
	wFile.Close()
	return nil
}

// Get min and max keys in the SSTable
func (sst *SSTable) GetMinMax() (string, string) {
	return sst.minKey, sst.maxKey
}

func (sst *SSTable) ResetFilePointer() {
	sst.file.Seek(0, io.SeekStart)
}

func (sst *SSTable) Index() map[string]uint32 {
	return sst.index
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

func (sst *SSTable) Len() int {
	return len(sst.index)
}
