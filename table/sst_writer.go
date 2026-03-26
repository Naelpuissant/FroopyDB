package table

import (
	"bufio"
	"froopydb/bloom"
	"froopydb/skiplist"
	"froopydb/x"
	"io"
	"os"
)

type SSTWriter struct {
	file   *os.File
	writer *bufio.Writer
	Pos    int64
}

func NewSSTWriter(file *os.File) *SSTWriter {
	pos, _ := file.Seek(0, io.SeekCurrent)
	return &SSTWriter{
		file:   file,
		writer: bufio.NewWriter(file),
		Pos:    pos,
	}
}

// WriteDataBlock writes a data block (value) to the SSTable.
func (w *SSTWriter) WriteDataBlock(value []byte) error {
	vlen := uint16(len(value))
	vlenBytes := x.Uint16ToBytes(vlen)

	_, err := w.writer.Write(vlenBytes)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(value)
	if err != nil {
		return err
	}

	w.Pos += int64(VLEN_SIZE + int(vlen))

	return nil
}

// WriteIndex writes the index map to the SSTable
// and set bloom filter key
func (w *SSTWriter) WriteIndex(index *skiplist.Skiplist, bf *bloom.BloomFilter) error {
	for key, offset := range index.KVIter() {
		bf.Add(key)
		klen := uint16(len(key))
		klenBytes := x.Uint16ToBytes(klen)
		_, err := w.writer.Write(klenBytes)
		if err != nil {
			return err
		}

		_, err = w.writer.Write([]byte(key))
		if err != nil {
			return err
		}

		_, err = w.writer.Write(offset)
		if err != nil {
			return err
		}

		w.Pos += int64(KLEN_SIZE + int(klen) + OFFSET_SIZE)
	}

	return nil
}

// WriteBloomFilter create a byte array from bloom filter' bitmap and write it
func (w *SSTWriter) WriteBloomFilter(bf *bloom.BloomFilter) error {
	buf := bf.Bytes()

	_, err := w.writer.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

// WriteMetadata writes the SSTable metadata at the end of the file.
func (w *SSTWriter) WriteMetadata(level uint16, incr uint16, idxOffset uint32, bfOffset uint32) error {
	metadataBytes := make([]byte, METADATA_SIZE)

	copy(metadataBytes[0:LEVEL_SIZE], x.Uint16ToBytes(level))
	copy(metadataBytes[LEVEL_SIZE:LEVEL_SIZE+INCR_SIZE], x.Uint16ToBytes(incr))
	copy(
		metadataBytes[LEVEL_SIZE+INCR_SIZE:METADATA_SIZE-BF_OFFSET_SIZE],
		x.Uint32ToBytes(idxOffset),
	)
	copy(
		metadataBytes[LEVEL_SIZE+INCR_SIZE+IDX_OFFSET_SIZE:METADATA_SIZE],
		x.Uint32ToBytes(bfOffset),
	)

	if _, err := w.writer.Write(metadataBytes); err != nil {
		return err
	}

	w.Pos += int64(METADATA_SIZE)
	return nil
}

func (w *SSTWriter) Flush() error {
	return w.writer.Flush()
}
