package froopydb_test

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	fpdb "froopydb"
	"froopydb/logger"
	"froopydb/x"
)

var memTableMaxSize = fpdb.KB

func TestGetSet(t *testing.T) {
	dir := t.TempDir()
	db := fpdb.NewDB(&fpdb.DBConfig{
		Folder:          dir,
		MemTableMaxSize: memTableMaxSize,
		ClearOnStart:    true,
		LogLevel:        logger.ERROR,
	})
	defer db.Close()

	db.Set(x.EncodeKey([]byte("1"), 0), []byte("foo"))
	db.Set(x.EncodeKey([]byte("1"), 0), []byte("bar"))
	result, found := db.Get(x.EncodeKey([]byte("1"), 0))
	if !found || string(result) != "bar" {
		t.Fatalf("Updated key 1 must be 'bar', found=%v", found)
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

	db.Set(x.EncodeKey([]byte("1"), 0), []byte("foo"))
	db.Set(x.EncodeKey([]byte("1"), 0), []byte("bar"))

	// Trigger a flush
	db.Set(x.EncodeKey([]byte(strconv.Itoa(2)), 0), []byte(strings.Repeat("a", memTableMaxSize)))

	result, found := db.Get(x.EncodeKey([]byte("1"), 0))
	if !found || string(result) != "bar" {
		t.Fatalf("Updated key 1 must be 'bar': %s, found=%v", result, found)
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

	db.Set(x.EncodeKey([]byte("1"), 0), []byte("foo"))

	db.Delete(x.EncodeKey([]byte("1"), 0))
	result, found := db.Get(x.EncodeKey([]byte("1"), 0))
	if found || string(result) != "" {
		t.Fatalf("Key 1 must be deleted: %s, found=%v", result, found)
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
		db.Set(x.EncodeKey([]byte(key), 0), []byte("foo"))
	}

	db.WaitFlush()

	result := db.Range(x.EncodeKey([]byte("010"), 0), x.EncodeKey([]byte("020"), 0))
	if result.Length() != 11 {
		t.Fatalf("Range should return 11 items, got %d", result.Length())
	}

	result = db.Range(x.EncodeKey([]byte("090"), 0), x.EncodeKey([]byte("099"), 0))
	if result.Length() != 10 {
		t.Fatalf("Range should return 10 items, got %d", result.Length())
	}

	result = db.Range(x.EncodeKey([]byte("200"), 0), x.EncodeKey([]byte("300"), 0))
	if result.Length() != 0 {
		t.Fatalf("Range should return 0 items, got %d", result.Length())
	}

	result = db.Range(x.EncodeKey([]byte("000"), 0), x.EncodeKey([]byte("099"), 0))
	if result.Length() != 100 {
		t.Fatalf("Range should return 100 items, got %d", result.Length())
	}

	result = db.Range(x.EncodeKey([]byte("050"), 0), x.EncodeKey([]byte("040"), 0))
	if result.Length() != 0 {
		t.Fatalf("Range with fromKey > toKey should return 0 items, got %d", result.Length())
	}

	db.Delete(x.EncodeKey([]byte("001"), 0))
	result = db.Range(x.EncodeKey([]byte("000"), 0), x.EncodeKey([]byte("002"), 0))
	if result.Length() != 2 {
		t.Fatalf("Range should return 2 items after deletion, got %d", result.Length())
	}

	db.Set(x.EncodeKey([]byte("002"), 0), []byte("bar"))
	result = db.Range(x.EncodeKey([]byte("002"), 0), x.EncodeKey([]byte("002"), 0))
	updatedValue, _ := result.First().Value, result.First() != nil
	if result.Length() != 1 || string(updatedValue) != "bar" {
		t.Fatalf("Range should return 1 item after setting key 002 to 'bar', got %d and value '%s'", result.Length(), string(updatedValue))
	}

	result = db.Range(x.EncodeKey([]byte("1"), 0), x.EncodeKey([]byte("0"), 0))
	if result.Length() != 0 {
		t.Fatalf("Range with fromKey > toKey should return 0 items, got %d", result.Length())
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
		db.Set(x.EncodeKey([]byte{byte(i)}, 0), []byte("pad"))
	}

	db.Set(x.EncodeKey([]byte("1"), 0), []byte("foo"))
	db.Set(x.EncodeKey([]byte("2"), 0), []byte("baz"))
	db.Set(x.EncodeKey([]byte("3"), 0), []byte("boo"))
	db.Delete(x.EncodeKey([]byte("2"), 0))
	db.Delete(x.EncodeKey([]byte("3"), 0))
	db.Set(x.EncodeKey([]byte("2"), 0), []byte("hey!"))

	db.Compact()

	result, found := db.Get(x.EncodeKey([]byte("1"), 0))
	if !found || string(result) != "foo" {
		t.Fatalf("Key 1 must foo: %s, found=%v", result, found)
	}

	result, found = db.Get(x.EncodeKey([]byte("3"), 0))
	if found || string(result) != "" {
		t.Fatalf("Key 3 must be deleted: %s, found=%v", result, found)
	}
}

func BenchmarkSet(b *testing.B) {
	dir := b.TempDir()
	db := fpdb.NewDB(fpdb.DefaultConfig(dir))
	defer db.Close()

	for b.Loop() {
		db.Set(x.EncodeKey([]byte(strconv.Itoa(b.N)), 0), []byte("load"))
	}
}

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	db := fpdb.NewDB(fpdb.DefaultConfig(dir))
	defer db.Close()

	// Populate the database
	for i := 0; i < b.N; i++ {
		db.Set(x.EncodeKey([]byte(strconv.Itoa(i)), 0), []byte("load"))
	}

	for b.Loop() {
		db.Get(x.EncodeKey([]byte(strconv.Itoa(b.N)), 0))
	}
}
