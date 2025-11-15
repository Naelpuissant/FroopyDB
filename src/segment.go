package src

import (
	"fmt"
	"io"
	"os"
	"sort"
)

type Segment struct {
	name  string
	size  int
	file  *os.File
	index map[[4]byte]uint32 // uint32 key
}

func newSegmentName(folder string, segmentIncr int) string {
	return fmt.Sprintf("%s/%d.sst", folder, segmentIncr)
}

func NewSegment(name string, size int) *Segment {
	return &Segment{
		name:  name,
		size:  size,
		index: map[[4]byte]uint32{},
	}
}

func (seg *Segment) WriteBlock(key [4]byte, value []byte) {
	offset, _ := seg.file.Seek(0, io.SeekCurrent)

	vlen := Uint16ToBytes(uint16(len(value)))

	seg.file.Write(vlen)
	seg.file.Write(value)

	if len(value) != 0 && value[0] != 0x00 {
		seg.index[key] = uint32(offset)
	}

	seg.size += 16 + len(value)
}

func (seg *Segment) WriteIndices() {
	indexOffset, _ := seg.file.Seek(0, io.SeekCurrent)
	for key, offset := range seg.index {
		klen := Uint16ToBytes(uint16(len(key)))
		seg.file.Write(klen)
		seg.file.Write(key[:])
		seg.file.Write(Uint32ToBytes(offset))
		seg.size += 16 + len(key) + 32
	}
	seg.file.Write(Uint32ToBytes(uint32(indexOffset)))
	seg.size += 32
}

func (seg *Segment) Open() *os.File {
	file, _ := os.OpenFile(seg.name, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	seg.file = file
	return file
}

func (seg *Segment) Close() {
	seg.file.Close()
}

func (seg *Segment) Remove() {
	seg.Close()
	os.Remove(seg.name)
}

func (seg *Segment) Rename(new string) {
	os.Rename(seg.name, new)
	seg.name = new
}

func (seg *Segment) DeleteIndex(key [4]byte) {
	delete(seg.index, key)
}

func (seg *Segment) Search(key [4]byte) []byte {
	offset, found := seg.index[key]
	if !found {
		return []byte{}
	}
	vlen := make([]byte, 2)
	seg.file.ReadAt(vlen, int64(offset))

	value := make([]byte, BytesToUint16(vlen))
	seg.file.ReadAt(value, int64(offset)+2)

	return value
}

type SegmentStore struct {
	segments       []*Segment // Map maybe better
	folder         string
	segmentMaxSize int
}

func NewSegmentStore(folder string, segmentMaxSize int) *SegmentStore {
	segments := []*Segment{}
	return &SegmentStore{
		segments:       segments,
		folder:         folder,
		segmentMaxSize: segmentMaxSize,
	}
}

func (store *SegmentStore) AddNew() *Segment {
	name := newSegmentName(store.folder, len(store.segments))
	seg := NewSegment(name, 0)
	store.segments = append(store.segments, seg)
	return seg
}

func (store *SegmentStore) CloseAll() {
	for _, seg := range store.segments {
		seg.Close()
	}
}

func (store *SegmentStore) Last() *Segment {
	return store.segments[len(store.segments)-1]
}

func (store *SegmentStore) Remove(seg *Segment) {
	for i := len(store.segments) - 1; i >= 0; i-- {
		if store.segments[i].name == seg.name {
			store.segments = append(store.segments[:i], store.segments[i+1:]...)
			seg.Remove()
			return
		}
	}
}

func (store *SegmentStore) Replace(oldSeg, newSeg *Segment) {
	for i := len(store.segments) - 1; i >= 0; i-- {
		if store.segments[i].name == oldSeg.name {
			oldSeg.Remove()
			store.segments[i] = newSeg

			// Keep it sorted... meh
			sort.Slice(store.segments, func(a, b int) bool {
				return store.segments[a].name < store.segments[b].name
			})
			return
		}
	}
}

func (store *SegmentStore) Search(key [4]byte) []byte {
	for i := len(store.segments) - 1; i >= 0; i-- {
		value := store.segments[i].Search(key)
		if len(value) != 0 {
			return value
		}
	}
	return []byte{}
}

func (store *SegmentStore) DeleteIndex(key [4]byte) {
	for _, seg := range store.segments {
		seg.DeleteIndex(key)
	}
}
