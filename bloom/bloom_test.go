package bloom_test

import (
	"crypto/sha1"
	"hash"
	"testing"

	"froopydb/bloom"
)

func TestBloomFilter(t *testing.T) {
	bloom := bloom.New(0.01, 4, func() hash.Hash {
		return sha1.New()
	})

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
