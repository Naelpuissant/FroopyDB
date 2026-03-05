package froopydb_test

import (
	"testing"

	"froopydb/x"

	"froopydb"
)

func TestTxn(t *testing.T) {
	println("Running TestTxn")
	db := froopydb.NewDB(froopydb.DefaultConfig(t.TempDir()))
	defer db.Close()

	txn := db.NewTransaction()
	txn.Set(x.IntKey(1), []byte("hello"))
	txn.Set(x.IntKey(2), []byte("world"))

	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn2 := db.NewTransaction()
	value := txn2.Get(x.IntKey(1))

	if string(value) != "hello" {
		t.Fatalf("Expected foo to be hello, got %s", string(value))
	}

	if string(txn2.Get(x.IntKey(2))) != "world" {
		t.Fatalf("Expected bar to be world, got %s", string(value))
	}

	if err := txn2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn3 := db.NewTransaction()
	txn3.Delete(x.IntKey(1))

	if txn3.Get(x.IntKey(1)) != nil {
		t.Fatalf("Expected key to be deleted")
	}

	if err := txn3.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn4 := db.NewTransaction()
	if txn4.Get(x.IntKey(1)) != nil {
		t.Fatalf("Expected key to be deleted")
	}

	if err := txn4.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
