package table

import (
	"fmt"
	"froopydb/x"
	"iter"
	"os"
)

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

func (r *SSTReader) SetMetadata() {
	metadataStart := r.fsize - int64(METADATA_SIZE)
	metadataBytes := make([]byte, METADATA_SIZE)
	r.file.ReadAt(metadataBytes, metadataStart)

	level := x.BytesToUint16(metadataBytes[0:LEVEL_SIZE])
	incr := x.BytesToUint16(metadataBytes[LEVEL_SIZE : LEVEL_SIZE+INCR_SIZE])

	idxOffset := x.BytesToUint32(
		metadataBytes[LEVEL_SIZE+INCR_SIZE : METADATA_SIZE-BF_OFFSET_SIZE],
	)
	bfOffset := x.BytesToUint32(
		metadataBytes[LEVEL_SIZE+INCR_SIZE+IDX_OFFSET_SIZE : METADATA_SIZE],
	)

	r.Metadata = &SSTMetadata{
		Level:     level,
		Incr:      incr,
		IdxOffset: idxOffset,
		BfOffset:  bfOffset,
	}
}

func (r SSTReader) ReadBloomFilter() []byte {
	end := r.fsize - int64(METADATA_SIZE)
	start := int64(r.Metadata.BfOffset)
	size := end - start

	b := make([]byte, size)
	_, err := r.file.ReadAt(b, start)
	if err != nil {
		panic(err)
	}

	return b
}

func (r *SSTReader) IndexIter() iter.Seq2[*IdxItem, error] {
	return func(yield func(*IdxItem, error) bool) {
		endIdxOffset := int64(r.Metadata.BfOffset)
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

func (r *SSTReader) ReadValueAtOffset(offset int64) ([]byte, error) {
	vlen := make([]byte, 2)
	if _, err := r.file.ReadAt(vlen, int64(offset)); err != nil {
		return []byte{}, err
	}

	value := make([]byte, x.BytesToUint16(vlen))
	if _, err := r.file.ReadAt(value, int64(offset)+2); err != nil {
		return []byte{}, err
	}

	return value, nil
}
