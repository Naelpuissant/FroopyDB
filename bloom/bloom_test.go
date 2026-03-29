package bloom_test

import (
	"bytes"
	"testing"

	"froopydb/bloom"
)

func TestBloomFilter(t *testing.T) {
	bf := bloom.New(0.01, 4)

	keys := []string{"hello", "world", "foo", "bar"}
	for _, key := range keys {
		bf.Add([]byte(key))
	}

	for _, key := range keys {
		if !bf.Contains([]byte(key)) {
			t.Errorf("Expected BloomFilter to contain key %s, but it does not", key)
		}
	}

	if bf.Contains([]byte("not in bloom")) {
		t.Fatalf("BloomFilter should not contain key 'not in bloom', but it does")
	}

	buf := bf.Bytes()
	bf2 := bloom.FromBytes(buf, 4)
	buf2 := bf2.Bytes()

	if !bytes.Equal(buf, buf2) {
		t.Fatalf("Those buffers should be the same, got %v and %v", buf, buf2)
	}

	for _, key := range keys {
		if !bf2.Contains([]byte(key)) {
			t.Fatalf("Expected BloomFilter to contain key %s, but it does not", key)
		}
	}

	if bf2.Contains([]byte("not in bloom")) {
		t.Fatalf("BloomFilter should not contain key 'not in bloom', but it does")
	}
}
