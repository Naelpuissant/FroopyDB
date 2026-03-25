package bloom

import (
	"encoding/binary"
	"errors"
	"hash"
	"math"
)

var (
	ErrWrongFalsePositive = errors.New("falsePositive should be between 0 and 1")
	ErrWrongNItems        = errors.New("nItems should be greater than one")
)

type BloomFilter struct {
	bitmap    *BitMap
	nbits     int
	nhashes   int
	newHash   func() hash.Hash
	digestBuf [16]byte
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

// Create a new BloomFilter newHash should return your hash function,
// please use a fast, non-cryptographic one
func New(falsePositive float64, nItems int, newHash func() hash.Hash) *BloomFilter {
	if falsePositive <= 0 || falsePositive >= 1 {
		panic(ErrWrongFalsePositive)
	}

	if nItems <= 1 {
		panic(ErrWrongNItems)
	}

	nbits := getNBits(falsePositive, float64(nItems))
	nhashes := getNHashes(float64(nbits), float64(nItems))

	bitmap, err := NewBitmap(nbits)
	if err != nil {
		panic(err)
	}

	return &BloomFilter{
		bitmap:  bitmap,
		nbits:   nbits,
		nhashes: int(nhashes),
		newHash: newHash,
	}
}

func (bf *BloomFilter) Add(key []byte) {
	hash := bf.newHash()
	digestBuf := [16]byte{}

	hash.Write(key)
	digest := hash.Sum(digestBuf[:])

	h1 := binary.BigEndian.Uint64(digest[:8])
	h2 := binary.BigEndian.Uint64(digest[8:16])
	h2 |= 1 // avoid 0 h2

	nbits := uint64(bf.nbits)
	for i := range bf.nhashes {
		idx := (h1 + uint64(i)*h2) % nbits
		bf.bitmap.Set(int(idx))
	}
}

func (bf *BloomFilter) Contains(key []byte) bool {
	hash := bf.newHash()
	digestBuf := [16]byte{}

	hash.Write(key)
	digest := hash.Sum(digestBuf[:])

	h1 := binary.BigEndian.Uint64(digest[:8])
	h2 := binary.BigEndian.Uint64(digest[8:16])
	h2 |= 1 // avoid 0 h2

	nbits := uint64(bf.nbits)
	for i := range bf.nhashes {
		idx := (h1 + uint64(i)*h2) % nbits
		if !bf.bitmap.IsSet(int(idx)) {
			return false
		}
	}

	return true
}
