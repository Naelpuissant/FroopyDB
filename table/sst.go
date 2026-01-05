package table

import (
	"bufio"
	"fmt"
	"froopydb/logger"
	"froopydb/x"
	"io"
	"os"
	"path/filepath"
	"strconv"
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

// ParseSSTableName parses "<folder>/<level>_<incr>.sst"
// Might want to store metadata elsewhere later
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

type SSTable struct {
	logger *logger.Logger

	name   string
	folder string
	level  int
	incr   int
	size   int
	file   *os.File
	index  map[[4]byte]uint32 // uint32 key
}

func NewSSTable(logger *logger.Logger, folder string, level int, incr int, tmp bool, size int) *SSTable {
	name := newSSTableName(folder, level, incr, tmp)
	return &SSTable{
		logger: logger,
		name:   name,
		folder: folder,
		level:  level,
		incr:   incr,
		size:   size,
		index:  map[[4]byte]uint32{},
	}
}

func NewSSTableFromFile(logger *logger.Logger, file *os.File) (*SSTable, error) {
	fstat, err := file.Stat()
	if err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("%w: %d/%d", ErrSSTableIndexRecoveryFailed, startOffset, endOffset)
	}

	logger.Debug("Recovered SSTable index", "size", len(index))

	folder, level, incr := parseSSTableName(file.Name())
	return &SSTable{
		logger: logger,
		size:   int(indexBlockSize),
		folder: folder,
		level:  level,
		incr:   incr,
		name:   file.Name(),
		index:  index,
		file:   file,
	}, nil
}

func (sst *SSTable) WriteBlock(key [4]byte, value []byte) error {
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
		sst.index[key] = uint32(offset)
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
		if _, err := w.Write(key[:]); err != nil {
			return err
		}
		if _, err := w.Write(x.Uint32ToBytes(offset)); err != nil {
			return err
		}
		sst.size += 16 + len(key) + 32
	}

	if _, err := w.Write(x.Uint32ToBytes(uint32(indexOffset))); err != nil {
		return err
	}

	sst.size += 32

	return w.Flush()
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

func (sst *SSTable) DeleteIndex(key [4]byte) {
	delete(sst.index, key)
}

func (sst *SSTable) Search(key [4]byte) ([]byte, error) {
	offset, found := sst.index[key]
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

func (sst *SSTable) ResetFilePointer() {
	sst.file.Seek(0, io.SeekStart)
}

func (sst *SSTable) Index() map[[4]byte]uint32 {
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
