package bloom

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	ErrWrongFalsePositive = errors.New("falsePositive should be between 0 and 1")
	ErrWrongNItems        = errors.New("nItems should be greater than zero")
	ErrIsImmutable        = errors.New("can't modify immutable bloom filter")
)

type BloomFilter struct {
	bitmap      *BitMap
	nbits       int
	nhashes     int
	digestBuf   [4]byte
	isImmutable bool
}

func getNBits(falsePositive float64, nItems float64) int {
	nbits := int(-(nItems * math.Log(falsePositive)) / math.Pow(math.Ln2, 2))

	// round up to nearest multiple of 64
	// divid and round up to get number of 64 bit blocs
	// multiply by 64 to get total bits
	return (nbits + 64 - 1) / 64 * 64
}

func getNHashes(nbits float64, nItems float64) int {
	nhashes := nbits / nItems * math.Ln2
	return int(math.Ceil(nhashes))
}

// Create a new BloomFilte
func New(falsePositive float64, nItems int) *BloomFilter {
	if falsePositive <= 0 || falsePositive >= 1 {
		panic(ErrWrongFalsePositive)
	}

	if nItems <= 0 {
		panic(ErrWrongNItems)
	}

	nbits := getNBits(falsePositive, float64(nItems))
	nhashes := getNHashes(float64(nbits), float64(nItems))

	bitmap, err := NewBitmap(nbits)
	if err != nil {
		panic(err)
	}

	return &BloomFilter{
		bitmap:      bitmap,
		nbits:       nbits,
		nhashes:     int(nhashes),
		isImmutable: false,
	}
}

func FromBytes(b []byte, nItems int) *BloomFilter {
	bitmap, err := NewBitmapFromBytes(b)
	if err != nil {
		panic(err)
	}
	nhashes := getNHashes(float64(bitmap.Size()), float64(nItems))
	return &BloomFilter{
		bitmap:      bitmap,
		nbits:       bitmap.Size(),
		nhashes:     nhashes,
		isImmutable: true,
	}
}

func (bf *BloomFilter) Add(key []byte) {
	if bf.isImmutable {
		panic(ErrIsImmutable)
	}

	digest := binary.BigEndian.AppendUint32(bf.digestBuf[:0], Hash(key))

	h1 := binary.BigEndian.Uint16(digest[:2])
	h2 := binary.BigEndian.Uint16(digest[2:4])
	h2 |= 1 // avoid 0 h2

	nbits := uint16(bf.nbits)
	for i := range bf.nhashes {
		idx := (h1 + uint16(i)*h2) % nbits
		bf.bitmap.Set(int(idx))
	}
}

// Contains returns :
// true if a key is "maybe" in the bloom filter,
// false if a key is not the bloom filter
func (bf *BloomFilter) Contains(key []byte) bool {
	digest := binary.BigEndian.AppendUint32(bf.digestBuf[:0], Hash(key))

	h1 := binary.BigEndian.Uint16(digest[:2])
	h2 := binary.BigEndian.Uint16(digest[2:4])
	h2 |= 1 // avoid 0 h2

	nbits := uint16(bf.nbits)
	for i := range bf.nhashes {
		idx := (h1 + uint16(i)*h2) % nbits
		if !bf.bitmap.IsSet(int(idx)) {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) Bytes() []byte { return bf.bitmap.Bytes() }

func (bf *BloomFilter) String() string { return bf.bitmap.String() }

func (bf *BloomFilter) Size() int { return bf.bitmap.Size() }
