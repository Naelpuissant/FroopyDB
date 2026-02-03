package froopydb_test

import (
	fpdb "froopydb"
	"froopydb/logger"
	"path/filepath"
	"strconv"
	"testing"
)

var (
	memTableMaxSize = fpdb.KB
)

func TestGetSet(t *testing.T) {
	dir := t.TempDir()
	db := fpdb.NewDB(dir, memTableMaxSize, true, logger.ERROR)
	defer db.Close()

	db.Set([]byte("1"), []byte("foo"))
	db.Set([]byte("1"), []byte("bar"))
	result := db.Get([]byte("1"))
	if string(result) != "bar" {
		t.Fatalf("Updated key 1 must be 'bar'")
	}

	metrics := db.Metrics()
	if metrics.TotalKeys != 1 && metrics.MemTableSize != 4 {
		t.Logf("Metrics: %+v", metrics)
		t.Fatalf("DB total keys should be 1 and memtable size should be 4, got %d keys and %d size", metrics.TotalKeys, metrics.MemTableSize)
	}
}

func TestGetMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	dir = filepath.Dir(dir)

	db := fpdb.NewDB(dir, memTableMaxSize, true, logger.ERROR)
	defer db.Close()

	db.Set([]byte("1"), []byte("foo"))
	db.Set([]byte("1"), []byte("bar"))

	// Trigger a flush
	for i := 2; i < memTableMaxSize/2; i++ {
		db.Set([]byte(strconv.Itoa(i)), []byte{'x'})
	}

	result := db.Get([]byte("1"))
	if string(result) != "bar" {
		t.Fatalf("Updated key 1 must be 'bar': %s", result)
	}

	db.WaitFlush()

	metrics := db.Metrics()
	if metrics.NumSSTables != 1 || metrics.DiskStorage == 0 {
		t.Fatalf("DB should have 1 SSTable and non-zero disk storage, got %d SSTables and %d disk storage", metrics.NumSSTables, metrics.DiskStorage)
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	db := fpdb.NewDB(dir, memTableMaxSize, true, logger.ERROR)
	defer db.Close()

	db.Set([]byte("1"), []byte("foo"))

	db.Delete([]byte("1"))
	result := db.Get([]byte("1"))
	if string(result) != "" {
		t.Fatalf("Key 1 must be deleted: %s", result)
	}
}

func TestCompactAndMerge(t *testing.T) {
	dir := t.TempDir()
	db := fpdb.NewDB(dir, 100, true, logger.ERROR)
	defer db.Close()

	for i := range 100 {
		db.Set([]byte{byte(i)}, []byte("pad"))
	}

	db.Set([]byte("1"), []byte("foo"))
	db.Set([]byte("2"), []byte("baz"))
	db.Set([]byte("3"), []byte("boo"))
	db.Delete([]byte("2"))
	db.Delete([]byte("3"))
	db.Set([]byte("2"), []byte("hey!"))

	db.Compact()

	result := db.Get([]byte("1"))
	if string(result) != "foo" {
		t.Fatalf("Key 1 must foo: %s", result)
	}

	result = db.Get([]byte("3"))
	if string(result) != "" {
		t.Fatalf("Key 3 must be deleted: %s", result)
	}
}

// func BenchmarkSet(b *testing.B) {
// 	for i := 0; i <= b.N; i++ {
// 		db.Set([]byte{byte(i)}, []byte("load"))
// 	}
// }

// func BenchmarkGet(b *testing.B) {
// 	for i := 0; i <= b.N; i++ {
// 		db.Get([]byte{byte(i)})
// 	}
// }
