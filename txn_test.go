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
		t.Fatalf("Expected 1 to be hello, got %s", string(value))
	}
	if string(txn2.Get(x.IntKey(2))) != "world" {
		t.Fatalf("Expected 2 to be world, got %s", string(value))
	}
	if err := txn2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn3 := db.NewTransaction()
	txn3.Delete(x.IntKey(1))
	if value := txn3.Get(x.IntKey(1)); len(value) != 0 {
		t.Fatalf("Expected key to be deleted, got %s", string(value))
	}
	if err := txn3.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn4 := db.NewTransaction()
	if value := txn4.Get(x.IntKey(1)); len(value) != 0 {
		t.Fatalf("Expected key to be deleted, got %s", string(value))
	}
	if err := txn4.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// TestTxnSetConflict tests that two transactions
// that write the same key will conflict if one of
// the transactions commits before the other
func TestTxnSetConflict(t *testing.T) {
	db := froopydb.NewDB(froopydb.DefaultConfig(t.TempDir()))
	defer db.Close()

	txn1 := db.NewTransaction()
	txn1.Set(x.IntKey(1), []byte("bar"))

	txn2 := db.NewTransaction()
	txn2.Set(x.IntKey(1), []byte("baz"))

	if err := txn1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if err := txn2.Commit(); err != froopydb.ErrTxnConflict {
		t.Fatalf("Expected transaction conflict, but commit succeeded")
	}
}

// TestTxnGetConflict tests that a transaction that reads a key
// will always get the last key written at his own timestamp,
// even if another transaction has committed a write to the same
// key with a later timestamp
func TestTxnGetConflict(t *testing.T) {
	db := froopydb.NewDB(froopydb.DefaultConfig(t.TempDir()))
	defer db.Close()

	txn1 := db.NewTransaction()
	txn1.Set(x.IntKey(1), []byte("bar"))

	txn2 := db.NewTransaction()

	if err := txn1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if value := txn2.Get(x.IntKey(1)); len(value) != 0 {
		t.Fatalf("Expected value to be empty, got %s", string(value))
	}

	if err := txn2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
