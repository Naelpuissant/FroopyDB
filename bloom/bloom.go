package bloom

type BitSet struct {
	s []byte
}

// Create a new BitSet with a specific fixed size in byte
func NewBitSet(size uint16) *BitSet {
	return &BitSet{
		s: make([]byte, size),
	}
}

type BloomFilter struct {
	bitset BitSet
	size   uint32
	hashes uint32
}

func NewBloomFilter(size uint32, hashes uint32) *BloomFilter {
	return &BloomFilter{}
}
