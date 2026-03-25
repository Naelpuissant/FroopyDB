package bloom

import (
	"errors"
	"fmt"
	"strings"
)

func uint64ToStr(i uint64) string {
	return fmt.Sprintf("%064b", i)
}

var (
	ErrWrongBitMapIndex = errors.New("Wrong BitMap index")
	ErrWrongBitMapSize  = errors.New("BitMap size should be a multiple of 64")
)

type BitMap struct {
	m []uint64
}

func NewBitmap(size int) (*BitMap, error) {
	if size%64 != 0 {
		return nil, ErrWrongBitMapSize
	}
	blocs := size / 64
	return &BitMap{m: make([]uint64, blocs)}, nil
}

func (b *BitMap) getBlocForIndex(i int) (int, error) {
	if i >= len(b.m)*64 || i < 0 {
		return -1, fmt.Errorf("%w: map size %d, got %d", ErrWrongBitMapIndex, b.Size()-1, i)
	}

	bloc := i >> 6 // floor division by 64
	return bloc, nil
}

func (b *BitMap) Set(i int) error {
	blocIdx, err := b.getBlocForIndex(i)
	if err != nil {
		return err
	}

	shift := i % 64
	b.m[blocIdx] |= uint64(1 << shift)

	return nil
}

func (b *BitMap) IsSet(i int) bool {
	blocIdx, err := b.getBlocForIndex(i)
	if err != nil {
		return false
	}

	shift := i % 64
	if b.m[blocIdx]&(uint64(1)<<shift) == 0 {
		return false
	}

	return true
}

func (b *BitMap) String() string {
	var str strings.Builder
	for i := len(b.m) - 1; i >= 0; i-- {
		str.WriteString(uint64ToStr(b.m[i]))
	}
	return str.String()
}

func (b *BitMap) Size() int {
	return len(b.m) * 64
}
