package skiplist_test

import (
	"bytes"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"froopydb/skiplist"
	"froopydb/x"
)

func b(i int) []byte {
	k := x.IntKey(i)
	ts := 1337
	return x.EncodeKey(k, uint64(ts))
}

func TestSkiplist(t *testing.T) {
	list := skiplist.New()
	_ = list.Insert(b(5), b(1))
	_ = list.Insert(b(3), b(2))
	_ = list.Insert(b(8), b(3))
	_ = list.Insert(b(1), b(4))

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
	_ = list.Insert(b(3), b(1337))

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
			_ = list.Insert(b(i), b(i))
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

	for i := 1; bm.Loop(); i++ {
		_ = list.Insert(b(i), b(i))
		i++
	}
}

func BenchmarkSkiplistSearch(bm *testing.B) {
	list := skiplist.New()

	size := 10000
	rands := make([]int, size)

	for i := range size {
		_ = list.Insert(b(i), b(i))
		rands[i] = rand.Intn(size - 1)
	}

	for i := 1; bm.Loop(); i++ {
		list.Search(b(rands[i%size]))
	}
}
