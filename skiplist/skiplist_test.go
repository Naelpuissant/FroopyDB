package skiplist_test

import (
	"bytes"
	"encoding/binary"
	"froopydb/skiplist"
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

func b(i int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func TestSkiplist(t *testing.T) {
	list := skiplist.New()
	list.Insert(b(5), b(1))
	list.Insert(b(3), b(2))
	list.Insert(b(8), b(3))
	list.Insert(b(1), b(4))

	// Insert
	expectedKeys := [][]byte{b(1), b(3), b(5), b(8)}
	actualKeys := list.Keys()

	if !reflect.DeepEqual(expectedKeys, actualKeys) {
		t.Errorf("Expected keys %v, but got %v", expectedKeys, actualKeys)
	}

	// Search existing key
	node := list.Search(b(3))
	if node == nil || !bytes.Equal(node.Value, b(2)) {
		t.Errorf("Expected to find key 3 with value 2, but got %v", node)
	}

	// Search non-existing key
	node = list.Search(b(10))
	if node != nil {
		t.Errorf("Expected to not find key 10, but got %v", node)
	}

	// Update value
	expectedLen := list.Length()
	list.Insert(b(3), b(1337))

	if list.Length() != expectedLen {
		t.Errorf("Expected length %d, but got %d", expectedLen, list.Length())
	}

	node = list.Search(b(3))
	if node == nil || !bytes.Equal(node.Value, b(1337)) {
		t.Errorf("Expected updated value 1337, got %v", node)
	}

	first := list.First().Key
	if first == nil || !bytes.Equal(first, b(1)) {
		t.Errorf("First node key should be 1, got %d", first)
	}

	last := list.Last().Key
	if last == nil || !bytes.Equal(last, b(8)) {
		t.Errorf("Last node key should be 8, got %d", last)
	}

	rng := list.Range(b(1), b(8))
	expectedKeys = [][]byte{b(1), b(3), b(5)}
	for i := range expectedKeys {
		if !bytes.Equal(rng[i].Key, expectedKeys[i]) {
			t.Errorf("Expected keys %v, but got %v", expectedKeys, rng)
		}
	}
}

func TestSkiplistInsertConcurrency(t *testing.T) {
	list := skiplist.New()

	wg := sync.WaitGroup{}
	wg.Add(1000)

	for i := range 1000 {
		go func(i int) {
			list.Insert(b(i), b(i))
			wg.Done()
		}(i)
	}
	wg.Wait()

	for i := range 1000 {
		node := list.Search(b(i))
		if node == nil || !bytes.Equal(node.Value, b(i)) {
			t.Errorf("Expected to find key %d", i)
		}
	}
}

func BenchmarkSkiplistInsert(bm *testing.B) {
	list := skiplist.New()

	bm.ResetTimer()
	for i := 0; i < bm.N; i++ {
		list.Insert(b(i), b(i))
	}
}

func BenchmarkSkiplistSearch(bm *testing.B) {
	list := skiplist.New()

	size := 10000
	rands := make([]int, size)

	for i := range size {
		list.Insert(b(i), b(i))
		rands[i] = rand.Intn(size - 1)
	}

	bm.ResetTimer()
	for i := 0; i < bm.N; i++ {
		list.Search(b(rands[i%size]))
	}
}
