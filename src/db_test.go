package src_test

import (
	"froopydb/src"
	"os"
	"testing"
)

var db *src.DB

func TestMain(m *testing.M) {
	os.RemoveAll("../db/test")
	db = src.NewDB("../db/test", 5, 1000)

	code := m.Run()

	db.Close()

	os.Exit(code)
}

func TestGetSet(t *testing.T) {
	db.Set(1, "foo")
	db.Set(1, "bar")
	result := db.Get(1)
	if result != "bar" {
		t.Fatalf("Updated key 1 must be 'bar'")
	}
}

func TestGetMultipleSegments(t *testing.T) {
	db.Set(1, "foo")
	db.Set(1, "bar")

	for i := range 10 {
		db.Set(i+2, "pad")
	}

	result := db.Get(1)
	if result != "bar" {
		t.Fatalf("Updated key 1 must be 'bar': %s", result)
	}
}

func TestDelete(t *testing.T) {
	db.Set(1, "foo")

	db.Delete(1)
	result := db.Get(1)
	if result != "" {
		t.Fatalf("Key 1 must be deleted: %s", result)
	}
}

func TestCompactAndMerge(t *testing.T) {
	//resetDB(db)
	for i := range 100 {
		db.Set(i+2, "pad")
	}

	db.Set(1, "foo")
	db.Set(2, "baz")
	db.Set(3, "boo")
	db.Delete(2)
	db.Delete(3)
	db.Set(2, "hey!")

	result := db.Get(1)
	if result != "foo" {
		t.Fatalf("Key 1 must foo: %s", result)
	}
}

func BenchmarkSet(b *testing.B) {
	for i := 0; i <= b.N; i++ {
		db.Set(i, "load")
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i <= b.N; i++ {
		db.Get(i)
	}
}
