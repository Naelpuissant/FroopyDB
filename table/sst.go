package table

import (
	"bufio"
	"fmt"
	"froopydb/x"
	"io"
	"os"
	"path/filepath"
	"strings"
)

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
	index  map[string]uint32
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
	}
}

func NewSSTableFromFile(file *os.File) (*SSTable, error) {
	sstReader, err := NewSSTReader(file)
	if err != nil {
		return nil, fmt.Errorf("%w : %w", ErrSSTableIndexRecoveryFailed, err)
	}

	index := map[string]uint32{}
	for item, err := range sstReader.Index() {
		if err != nil {
			return nil, fmt.Errorf("%w : %w", ErrSSTableIndexRecoveryFailed, err)
		}
		index[string(item.Key)] = item.Offset
	}

	filename := file.Name()
	return &SSTable{
		size:   int(sstReader.Metadata.IdxOffset),
		folder: filepath.Dir(filename),
		level:  int(sstReader.Metadata.Level),
		incr:   int(sstReader.Metadata.Incr),
		name:   filename,
		index:  index,
		file:   file,
	}, nil
}

func (sst *SSTable) WriteBlock(key []byte, value []byte) error {
	offset, _ := sst.file.Seek(0, io.SeekCurrent)
	w := bufio.NewWriter(sst.file)
	vlen := x.Uint16ToBytes(uint16(len(value)))

	if _, err := w.Write(vlen); err != nil {
		return err
	}
	if _, err := w.Write(value); err != nil {
		return err
	}

	if len(value) != 0 && value[0] != 0x00 {
		sst.index[string(key)] = uint32(offset)
	}

	sst.size += 16 + len(value)

	return w.Flush()
}

func (sst *SSTable) WriteIndices() error {
	indexOffset, err := sst.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(sst.file)

	for key, offset := range sst.index {
		klen := x.Uint16ToBytes(uint16(len(key)))
		if _, err := w.Write(klen); err != nil {
			return err
		}
		if _, err := w.Write([]byte(key)); err != nil {
			return err
		}
		if _, err := w.Write(x.Uint32ToBytes(offset)); err != nil {
			return err
		}
		sst.size += 16 + len(key) + 32
	}

	// index offset pointer
	if _, err := w.Write(x.Uint32ToBytes(uint32(indexOffset))); err != nil {
		return err
	}

	sst.size += 32

	return w.Flush()
}

// Footer :
func (sst *SSTable) WriteFooter() error {

	return nil
}

func (sst *SSTable) Open() (*os.File, error) {
	file, err := os.OpenFile(sst.name, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
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
}

func (sst *SSTable) Search(key []byte) ([]byte, error) {
	offset, found := sst.index[string(key)]
	if !found {
		return []byte{}, nil
	}

	vlen := make([]byte, 2)
	if _, err := sst.file.ReadAt(vlen, int64(offset)); err != nil {
		return []byte{}, err
	}

	value := make([]byte, x.BytesToUint16(vlen))
	if _, err := sst.file.ReadAt(value, int64(offset)+2); err != nil {
		return []byte{}, err
	}

	return value, nil
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
	err := sst.setReadOnly()
	return err
}

func (sst *SSTable) setReadOnly() error {
	file, err := os.OpenFile(sst.name, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	wFile := sst.file
	sst.file = file
	wFile.Close()
	return nil
}

// Get min and max keys in the SSTable
// Might want to store this metadata elsewhere later
func (sst *SSTable) GetMinMax() (string, string) {
	if len(sst.index) == 0 {
		return "", ""
	}

	// Awfully inefficient, but whatever for now
	minKey := string([]byte{255, 255, 255, 255, 255, 255, 255, 255})
	maxKey := string([]byte{0, 0, 0, 0, 0, 0, 0, 0})

	for key := range sst.index {
		if key < minKey {
			minKey = key
		}
		if key > maxKey {
			maxKey = key
		}
	}

	return minKey, maxKey
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
