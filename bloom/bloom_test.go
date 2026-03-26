package bloom_test

import (
	"testing"

	"froopydb/bloom"
)

func TestBloomFilter(t *testing.T) {
	bloom := bloom.New(0.01, 4)

	keys := []string{"hello", "world", "foo", "bar"}
	for _, key := range keys {
		bloom.Add([]byte(key))
	}

	for _, key := range keys {
		if !bloom.Contains([]byte(key)) {
			t.Errorf("Expected BloomFilter to contain key %s, but it does not", key)
		}
	}
}

func TestBloomFilterBytes(t *testing.T) {
	bf := bloom.New(0.01, 4)

	keys := []string{"hello", "world", "foo", "bar"}
	for _, key := range keys {
		bf.Add([]byte(key))
	}

	buf := bf.Bytes()

	bf2 := bloom.FromBytes(buf)
	for _, key := range keys {
		if !bf2.Contains([]byte(key)) {
			t.Errorf("Expected BloomFilter to contain key %s, but it does not", key)
		}
	}
}
