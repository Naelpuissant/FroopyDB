package froopydb_test

import (
	"testing"

	"froopydb"
)

func TestTxn(t *testing.T) {
	db := froopydb.NewDB(froopydb.DefaultConfig(t.TempDir()))
	defer db.Close()

	txn := db.NewTransaction()
	txn.Set([]byte("foo"), []byte("hello"))
	txn.Set([]byte("bar"), []byte("world"))

	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if string(db.Get([]byte("foo"))) != "hello" {
		t.Fatalf("Expected foo to be hello, got %s", db.Get([]byte("foo")))
	}
	if string(db.Get([]byte("bar"))) != "world" {
		t.Fatalf("Expected bar to be world, got %s", db.Get([]byte("bar")))
	}
}
