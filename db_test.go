package froopydb_test

import (
	"fmt"
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
	db := fpdb.NewDB(&fpdb.DBConfig{
		Folder:          dir,
		MemTableMaxSize: memTableMaxSize,
		ClearOnStart:    true,
		LogLevel:        logger.ERROR,
	})
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

	db := fpdb.NewDB(&fpdb.DBConfig{
		Folder:          dir,
		MemTableMaxSize: memTableMaxSize,
		ClearOnStart:    true,
		LogLevel:        logger.ERROR,
	})
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
	db := fpdb.NewDB(&fpdb.DBConfig{
		Folder:          dir,
		MemTableMaxSize: memTableMaxSize,
		ClearOnStart:    true,
		LogLevel:        logger.ERROR,
	})
	defer db.Close()

	db.Set([]byte("1"), []byte("foo"))

	db.Delete([]byte("1"))
	result := db.Get([]byte("1"))
	if string(result) != "" {
		t.Fatalf("Key 1 must be deleted: %s", result)
	}
}

func TestRange(t *testing.T) {
	dir := t.TempDir()
	db := fpdb.NewDB(&fpdb.DBConfig{
		Folder:          dir,
		MemTableMaxSize: 128,
		ClearOnStart:    true,
		LogLevel:        logger.ERROR,
	})
	defer db.Close()

	for i := range 100 {
		key := fmt.Sprintf("%03d", i)
		db.Set([]byte(key), []byte("foo"))
	}

	db.WaitFlush()

	result := db.Range([]byte("010"), []byte("020"))
	if result.Len() != 11 {
		t.Fatalf("Range should return 11 items, got %d", result.Len())
	}

	result = db.Range([]byte("090"), []byte("099"))
	if result.Len() != 10 {
		t.Fatalf("Range should return 10 items, got %d", result.Len())
	}

	result = db.Range([]byte("200"), []byte("300"))
	if result.Len() != 0 {
		t.Fatalf("Range should return 0 items, got %d", result.Len())
	}

	result = db.Range([]byte("000"), []byte("099"))
	if result.Len() != 100 {
		t.Fatalf("Range should return 100 items, got %d", result.Len())
	}

	result = db.Range([]byte("050"), []byte("040"))
	if result.Len() != 0 {
		t.Fatalf("Range with fromKey > toKey should return 0 items, got %d", result.Len())
	}

	db.Delete([]byte("001"))
	result = db.Range([]byte("000"), []byte("002"))
	if result.Len() != 2 {
		t.Fatalf("Range should return 2 items after deletion, got %d", result.Len())
	}

	db.Set([]byte("002"), []byte("bar"))
	result = db.Range([]byte("002"), []byte("002"))
	updatedValue, _ := result.Front().Value.([]byte)
	if result.Len() != 1 || string(updatedValue) != "bar" {
		t.Fatalf("Range should return 1 item after setting key 002 to 'bar', got %d and value '%s'", result.Len(), string(updatedValue))
	}

	result = db.Range([]byte("1"), []byte("0"))
	if result.Len() != 0 {
		t.Fatalf("Range with fromKey > toKey should return 0 items, got %d", result.Len())
	}
}

func TestCompactAndMerge(t *testing.T) {
	dir := t.TempDir()
	db := fpdb.NewDB(&fpdb.DBConfig{
		Folder:          dir,
		MemTableMaxSize: 100,
		ClearOnStart:    true,
		LogLevel:        logger.ERROR,
	})
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

func BenchmarkSet(b *testing.B) {
	dir := b.TempDir()
	db := fpdb.NewDB(fpdb.DefaultConfig(dir))
	defer db.Close()

	for b.Loop() {
		db.Set([]byte(strconv.Itoa(b.N)), []byte("load"))
	}
}

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	db := fpdb.NewDB(fpdb.DefaultConfig(dir))
	defer db.Close()

	// Populate the database
	for i := 0; i < b.N; i++ {
		db.Set([]byte(strconv.Itoa(i)), []byte("load"))
	}

	for b.Loop() {
		db.Get([]byte(strconv.Itoa(b.N)))
	}
}
