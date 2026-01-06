package froopydb_test

import (
	fpdb "froopydb"
	"froopydb/logger"
	"os"
	"testing"
)

var db *fpdb.DB

func TestMain(m *testing.M) {
	os.RemoveAll("./db/test")
	db = fpdb.NewDB("./db/test", 5, fpdb.KB*128, true, logger.ERROR)

	code := m.Run()

	db.Close()

	os.Exit(code)
}

func TestGetSet(t *testing.T) {
	db.Set([]byte{1}, []byte("foo"))
	db.Set([]byte{1}, []byte("bar"))
	result := db.Get([]byte{1})
	if string(result) != "bar" {
		t.Fatalf("Updated key 1 must be 'bar'")
	}
}

func TestGetMultipleSegments(t *testing.T) {
	db.Set([]byte{1}, []byte("foo"))
	db.Set([]byte{1}, []byte("bar"))

	for i := range 10 {
		db.Set([]byte{byte(i + 2)}, []byte("pad"))
	}

	result := db.Get([]byte{1})
	if string(result) != "bar" {
		t.Fatalf("Updated key 1 must be 'bar': %s", result)
	}
}

func TestDelete(t *testing.T) {
	db.Set([]byte{1}, []byte("foo"))

	db.Delete([]byte{1})
	result := db.Get([]byte{1})
	if string(result) != "" {
		t.Fatalf("Key 1 must be deleted: %s", result)
	}
}

func TestCompactAndMerge(t *testing.T) {
	os.RemoveAll("./test/db")
	olddb := db
	db = fpdb.NewDB("./test/db", 5, 100, true, logger.ERROR)
	for i := range 100 {
		db.Set([]byte{byte(i)}, []byte("pad"))
	}

	db.Set([]byte{1}, []byte("foo"))
	db.Set([]byte{2}, []byte("baz"))
	db.Set([]byte{3}, []byte("boo"))
	db.Delete([]byte{2})
	db.Delete([]byte{3})
	db.Set([]byte{2}, []byte("hey!"))

	result := db.Get([]byte{1})
	if string(result) != "foo" {
		t.Fatalf("Key 1 must foo: %s", result)
	}

	result = db.Get([]byte{3})
	if string(result) != "" {
		t.Fatalf("Key 3 must be deleted: %s", result)
	}
	db = olddb
}

func BenchmarkSet(b *testing.B) {
	for i := 0; i <= b.N; i++ {
		db.Set([]byte{byte(i)}, []byte("load"))
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i <= b.N; i++ {
		db.Get([]byte{byte(i)})
	}
}
