package table

import (
	"fmt"
	"froopydb/x"
	"iter"
	"os"
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

type IdxItem struct {
	Key    []byte
	Offset uint32
}

type SSTReader struct {
	file     *os.File
	fsize    int64
	Metadata *SSTMetadata
}

func NewSSTReader(file *os.File) (*SSTReader, error) {
	fstat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	sstReader := &SSTReader{
		file:  file,
		fsize: fstat.Size(),
	}
	sstReader.SetMetadata()
	return sstReader, nil
}

func (r *SSTReader) Close() error {
	return r.file.Close()
}

func (r *SSTReader) SetMetadata() {
	metadataStart := r.fsize - int64(METADATA_SIZE)
	metadataBytes := make([]byte, METADATA_SIZE)
	r.file.ReadAt(metadataBytes, metadataStart)

	level := x.BytesToUint16(metadataBytes[0:LEVEL_SIZE])
	incr := x.BytesToUint16(metadataBytes[LEVEL_SIZE : LEVEL_SIZE+INCR_SIZE])
	idxOffset := x.BytesToUint32(metadataBytes[LEVEL_SIZE+INCR_SIZE : METADATA_SIZE])

	r.Metadata = &SSTMetadata{
		Level:     level,
		Incr:      incr,
		IdxOffset: idxOffset,
	}
}

func (r *SSTReader) Index() iter.Seq2[*IdxItem, error] {
	return func(yield func(*IdxItem, error) bool) {
		endIdxOffset := r.fsize - int64(METADATA_SIZE)
		curr := int64(r.Metadata.IdxOffset)

		for curr < endIdxOffset {
			klenBytes := make([]byte, 2)
			_, err := r.file.ReadAt(klenBytes, curr)
			if err != nil {
				yield(
					nil,
					fmt.Errorf("%w, klen is 2B but we are %d/%d", ErrSSTReaderIndexIterFailed, curr, endIdxOffset),
				)
				return
			}
			curr += 2

			klen := x.BytesToUint16(klenBytes)
			key := make([]byte, klen)
			_, err = r.file.ReadAt(key, curr)
			if err != nil {
				yield(
					nil,
					fmt.Errorf("%w, key is %dB but we are %d/%d", ErrSSTReaderIndexIterFailed, klen, curr, endIdxOffset),
				)
				return
			}
			curr += int64(klen)

			offset := make([]byte, 4)
			_, err = r.file.ReadAt(offset, curr)
			if err != nil {
				yield(
					nil,
					fmt.Errorf("%w, offset is 4B but we are %d/%d", ErrSSTReaderIndexIterFailed, curr, endIdxOffset),
				)
				return
			}
			curr += 4

			item := &IdxItem{
				Key:    key,
				Offset: x.BytesToUint32(offset),
			}

			if !yield(item, nil) {
				return
			}
		}
	}
}
