package table

import (
	"bytes"
	"fmt"
	"froopydb/x"
	"iter"
	"os"

	"github.com/edsrzf/mmap-go"
)

type IdxItem struct {
	Key    []byte
	Offset uint32
}

// Size returns the total size of the index item in bytes
// KLEN_SIZE + len(Key) + OFFSET_SIZE
func (i *IdxItem) Size() int64 { return int64(KLEN_SIZE + len(i.Key) + OFFSET_SIZE) }

type SSTReader struct {
	file     *bytes.Reader
	fsize    int64
	Metadata *SSTMetadata
}

func NewSSTReader(file *os.File) (*SSTReader, error) {
	fstat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	mf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(mf)

	sstReader := &SSTReader{
		file:  reader,
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
	nKeys := x.BytesToUint32(
		metadataBytes[LEVEL_SIZE+INCR_SIZE : LEVEL_SIZE+INCR_SIZE+NKEYS_SIZE],
	)
	idxOffset := x.BytesToUint32(
		metadataBytes[LEVEL_SIZE+INCR_SIZE+NKEYS_SIZE : LEVEL_SIZE+INCR_SIZE+NKEYS_SIZE+IDX_OFFSET_SIZE],
	)
	bfOffset := x.BytesToUint32(
		metadataBytes[LEVEL_SIZE+INCR_SIZE+NKEYS_SIZE+IDX_OFFSET_SIZE : METADATA_SIZE],
	)

	r.Metadata = &SSTMetadata{
		Level:     level,
		Incr:      incr,
		NKeys:     nKeys,
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

func (r SSTReader) GetIdxStartBlocksOffset() uint32 {
	return r.Metadata.BfOffset - r.Metadata.NKeys*uint32(IDX_START_BLOCKS_SIZE)
}

func (r SSTReader) ReadIndexAtOffset(offset int64) (*IdxItem, error) {
	klenBytes := make([]byte, 2)
	_, err := r.file.ReadAt(klenBytes, offset)
	if err != nil {
		return nil, fmt.Errorf("%w, klen is 2B but we are %d", ErrSSTReaderIndexIterFailed, offset)
	}
	offset += 2

	klen := x.BytesToUint16(klenBytes)
	key := make([]byte, klen)
	_, err = r.file.ReadAt(key, offset)
	if err != nil {
		return nil, fmt.Errorf("%w, key is %dB but we are %d", ErrSSTReaderIndexIterFailed, klen, offset)
	}
	offset += int64(klen)

	valOffset := make([]byte, 4)
	_, err = r.file.ReadAt(valOffset, offset)
	if err != nil {
		return nil, fmt.Errorf("%w, offset is 4B but we are %d", ErrSSTReaderIndexIterFailed, offset)
	}
	offset += 4

	return &IdxItem{
		Key:    key,
		Offset: x.BytesToUint32(valOffset),
	}, nil
}

func (r *SSTReader) IndexIter() iter.Seq2[*IdxItem, error] {
	return func(yield func(*IdxItem, error) bool) {
		endIdxOffset := int64(r.GetIdxStartBlocksOffset())
		curr := int64(r.Metadata.IdxOffset)

		for curr < endIdxOffset {
			idx, err := r.ReadIndexAtOffset(curr)
			if err != nil {
				yield(nil, err)
				return
			}
			curr += idx.Size()

			if !yield(idx, nil) {
				return
			}
		}
	}
}

func (r *SSTReader) Search(key []byte) (*IdxItem, bool) {
	return r.IdxBisectScan(key, 0, int(r.Metadata.NKeys))
}

func (r *SSTReader) IdxBisectScan(key []byte, start, end int) (*IdxItem, bool) {
	idxStartBlocks := r.GetIdxStartBlocksOffset()
	return r.doIdxBisectScan(key, 0, int(r.Metadata.NKeys)-1, idxStartBlocks)
}

func (r *SSTReader) doIdxBisectScan(key []byte, start, end int, idxStartBlocks uint32) (*IdxItem, bool) {
	if start > end {
		return nil, false
	}

	mid := (start + end) / 2
	midKeyStartBlockOffset := idxStartBlocks + uint32(mid)*uint32(IDX_START_BLOCKS_SIZE)
	idxItem, err := r.ReadIdxItemAtStartBlockOffset(int64(midKeyStartBlockOffset))
	if err != nil {
		panic(err)
	}

	searchKey, searchTs := x.DecodeKey(key)
	plainKey, ts := x.DecodeKey(idxItem.Key)
	keyCmp := bytes.Compare(plainKey, searchKey)

	if keyCmp < 0 {
		// Search higher key to the right of mid
		return r.doIdxBisectScan(key, mid+1, end, idxStartBlocks)
	}
	if keyCmp > 0 {
		// Search lower key to the left of mid
		return r.doIdxBisectScan(key, start, mid-1, idxStartBlocks)
	}
	if keyCmp == 0 {
		// Keys are equal, compare timestamps
		if ts == searchTs {
			return idxItem, true
		}
		if ts < searchTs {
			// Search to a highest timestamp to the right of mid
			candidate, found := r.doIdxBisectScan(key, mid+1, end, idxStartBlocks)
			if found {
				_, candidateTs := x.DecodeKey(candidate.Key)
				if candidateTs > ts {
					return candidate, true
				}
				return idxItem, true
			}
			return idxItem, true
		}
		if ts > searchTs {
			// Search to a lowest timestamp to the left of mid
			candidate, found := r.doIdxBisectScan(key, start, mid-1, idxStartBlocks)
			if found {
				_, candidateTs := x.DecodeKey(candidate.Key)
				if candidateTs < ts {
					return candidate, true
				}
			}
			return nil, false
		}
	}

	return nil, false
}

func (r *SSTReader) ReadIdxItemAtStartBlockOffset(startBlockOffset int64) (*IdxItem, error) {
	offset := make([]byte, 4)
	_, err := r.file.ReadAt(offset, startBlockOffset)
	if err != nil {
		return nil, err
	}
	return r.ReadIndexAtOffset(int64(x.BytesToUint32(offset)))
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

func (r *SSTReader) Range(from []byte, to []byte) []*IdxItem {
	if bytes.Compare(from, to) > 0 {
		return nil
	}

	idxItems := make([]*IdxItem, 0)

	r.IndexIter()(func(idxItem *IdxItem, err error) bool {
		if err != nil {
			return false
		}

		if bytes.Compare(idxItem.Key, from) >= 0 && bytes.Compare(idxItem.Key, to) <= 0 {
			idxItems = append(idxItems, idxItem)
		}

		if bytes.Compare(idxItem.Key, to) > 0 {
			return false
		}

		return true
	})

	return idxItems
}

func (r *SSTReader) GetMinKey() []byte {
	idxStartBlocks := r.GetIdxStartBlocksOffset()
	idxItem, err := r.ReadIdxItemAtStartBlockOffset(int64(idxStartBlocks))
	if err != nil {
		panic(err)
	}
	return idxItem.Key
}

func (r *SSTReader) GetMaxKey() []byte {
	idxStartBlocks := r.GetIdxStartBlocksOffset()
	lastIdxOffset := idxStartBlocks + uint32(r.Metadata.NKeys-1)*uint32(IDX_START_BLOCKS_SIZE)
	idxItem, err := r.ReadIdxItemAtStartBlockOffset(int64(lastIdxOffset))
	if err != nil {
		panic(err)
	}
	return idxItem.Key
}
