package bloom_test

import (
	"errors"
	"testing"

	"froopydb/bloom"
)

func TestBitMap(t *testing.T) {
	bm, _ := bloom.NewBitmap(256)
	bm.Set(7)

	_, err := bloom.NewBitmap(255)
	if err != bloom.ErrWrongBitMapSize {
		t.Fatalf("Expected error when creating BitMap of size 255, got %v", err)
	}

	bitmapStr := bm.String()
	c := bitmapStr[255-7] // standard bit representation (right to left)
	if c != '1' {
		t.Fatalf("BitMap[7] should be 1, got %b", c)
	}

	bm.Set(120)
	bitmapStr = bm.String()
	c = bitmapStr[255-120]
	if c != '1' {
		t.Fatalf("BitMap[120] should be 1, got %b", c)
	}

	bm.Set(7)
	if bm.String() != bitmapStr {
		t.Fatalf("BitMap should be unchanged after setting same bit, got %s", bm.String())
	}

	if !bm.IsSet(7) {
		t.Fatalf("BitMap[7] should be set")
	}

	bm.Set(0)
	if !bm.IsSet(0) {
		t.Fatalf("BitMap[0] should be set")
	}

	bm.Set(255)
	if !bm.IsSet(255) {
		t.Fatalf("BitMap[255] should be set")
	}

	err = bm.Set(256)
	if !errors.Is(err, bloom.ErrWrongBitMapIndex) {
		t.Fatalf("Expected error when setting bit index 256, got \"%v\"", err)
	}
}
