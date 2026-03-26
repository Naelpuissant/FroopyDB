package table

import (
	"fmt"
	"froopydb/bloom"
	"froopydb/skiplist"
	"froopydb/x"
	"iter"
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

	// Dynamic sized bloom filter between Index and Metadata

	// Metadata
	LEVEL_SIZE      = 2
	INCR_SIZE       = 2
	IDX_OFFSET_SIZE = 4
	BF_OFFSET_SIZE  = 4
	METADATA_SIZE   = LEVEL_SIZE + INCR_SIZE + IDX_OFFSET_SIZE + BF_OFFSET_SIZE
)

type SSTMetadata struct {
	Level     uint16
	Incr      uint16
	IdxOffset uint32
	BfOffset  uint32
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

	file   *os.File
	writer *SSTWriter
	reader *SSTReader

	index *skiplist.Skiplist
	bf    *bloom.BloomFilter
}

func NewSSTable(folder string, level int, incr int, tmp bool, size int) *SSTable {
	name := newSSTableName(folder, level, incr, tmp)
	return &SSTable{
		name:   name,
		folder: folder,
		level:  level,
		incr:   incr,
		size:   size,
		index:  skiplist.New(),
	}
}

func NewSSTableFromFile(file *os.File) (*SSTable, error) {
	sstReader, err := NewSSTReader(file)
	if err != nil {
		return nil, fmt.Errorf("%w : %w", ErrSSTableIndexRecoveryFailed, err)
	}

	index := skiplist.New()
	for item, err := range sstReader.IndexIter() {
		if err != nil {
			return nil, fmt.Errorf("%w : %w", ErrSSTableIndexRecoveryFailed, err)
		}
		index.Insert(item.Key, x.Uint32ToBytes(item.Offset))
	}
	bfBytes := sstReader.ReadBloomFilter()
	bf := bloom.FromBytes(bfBytes)

	filename := file.Name()
	return &SSTable{
		size:   int(sstReader.Metadata.IdxOffset),
		folder: filepath.Dir(filename),
		level:  int(sstReader.Metadata.Level),
		incr:   int(sstReader.Metadata.Incr),
		name:   filename,
		bf:     bf,
		index:  index,
		file:   file,
		reader: sstReader,
	}, nil
}

func (sst *SSTable) InitWriter() {
	sst.writer = NewSSTWriter(sst.file)
}

// WriteDataBlock writes value byte array,
// add key and offset to the index
func (sst *SSTable) WriteDataBlock(key, value []byte) error {
	offset := uint32(sst.writer.Pos)

	err := sst.writer.WriteDataBlock(value)
	if err != nil {
		return err
	}

	// Tombstone check
	if len(value) != 0 && value[0] != 0x00 {
		sst.index.Insert(key, x.Uint32ToBytes(offset))
	}

	return nil
}

// WriteIndex writes the index map to the SSTable,
// create bloom filter with ideal size and fill it.
// Returns the offset where it was written.
func (sst *SSTable) WriteIndex() (uint32, error) {
	indexOffset := uint32(sst.writer.Pos)

	sst.bf = bloom.New(0.1, int(sst.index.Length()))

	err := sst.writer.WriteIndex(sst.index, sst.bf)
	if err != nil {
		return 0, err
	}

	return indexOffset, nil
}

func (sst *SSTable) WriteBloomFilter() (uint32, error) {
	bfOffset := uint32(sst.writer.Pos)

	err := sst.writer.WriteBloomFilter(sst.bf)
	if err != nil {
		return 0, err
	}

	return bfOffset, nil
}

func (sst *SSTable) WriteMetadata(idxOffset uint32, bfOffset uint32) error {
	return sst.writer.WriteMetadata(
		uint16(sst.level), uint16(sst.incr), idxOffset, bfOffset,
	)
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

func (sst *SSTable) Search(key []byte) ([]byte, bool) {
	node, found := sst.index.Search(key)
	if !found {
		return []byte{}, false
	}

	value, err := sst.reader.ReadValueAtOffset(
		int64(x.BytesToUint32(node.Value)),
	)
	if err != nil {
		panic(err)
	}

	return value, true
}

func (sst *SSTable) Range(res map[string][]byte, fromKey, toKey []byte) {
	if sst.MinKey() > string(toKey) || sst.MaxKey() < string(fromKey) {
		return
	}

	rng := sst.index.Range(fromKey, toKey)

	for _, node := range rng {
		value, found := sst.Search(node.Key)
		if found {
			res[string(node.Key)] = value
		} else {
			delete(res, string(node.Key))
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

func (sst *SSTable) KVIter() iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for key, offset := range sst.index.KVIter() {
			value, err := sst.reader.ReadValueAtOffset(
				int64(x.BytesToUint32(offset)),
			)
			if err != nil {
				panic(err)
			}

			if !yield(string(key), value) {
				return
			}
		}
	}
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
	return int(sst.index.Length())
}

func (sst *SSTable) MinKey() string {
	return string(sst.index.First().Key)
}

func (sst *SSTable) MaxKey() string {
	return string(sst.index.Last().Key)
}
