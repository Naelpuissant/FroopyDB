package froopydb_test

import (
	"strings"
	"testing"

	"froopydb/logger"
	"froopydb/x"

	"froopydb"
)

func TestTxn(t *testing.T) {
	db := froopydb.NewDB(
		&froopydb.DBConfig{
			Folder:          t.TempDir(),
			MemTableMaxSize: memTableMaxSize,
			ClearOnStart:    true,
			LogLevel:        logger.INFO,
		},
	)
	defer db.Close()

	txn := db.NewTransaction()
	txn.Set(x.IntKey(1), []byte("hello"))
	txn.Set(x.IntKey(2), []byte("world"))
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn = db.NewTransaction()
	value, found := txn.Get(x.IntKey(1))
	if !found || string(value) != "hello" {
		t.Fatalf("Expected 1 to be hello, got %s (found=%v)", string(value), found)
	}
	value, found = txn.Get(x.IntKey(2))
	if !found || string(value) != "world" {
		t.Fatalf("Expected 2 to be world, got %s (found=%v)", string(value), found)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn = db.NewTransaction()
	txn.Delete(x.IntKey(1))
	value, found = txn.Get(x.IntKey(1))
	if found || len(value) != 0 {
		t.Fatalf("Expected key to be deleted, got %s (found=%v)", string(value), found)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn = db.NewTransaction()
	value, found = txn.Get(x.IntKey(1))
	if found || len(value) != 0 {
		t.Fatalf("Expected key to be deleted, got %s (found=%v)", string(value), found)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Make sure to flush
	txn = db.NewTransaction()
	txn.Set(x.IntKey(1337), []byte("flushed"))
	txn.Set(x.IntKey(3), []byte(strings.Repeat("a", memTableMaxSize)))
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	db.WaitJobs()

	// Get key from flushed txn
	txn = db.NewTransaction()
	value, found = txn.Get(x.IntKey(1337))
	if !found || string(value) != "flushed" {
		t.Fatalf("Expected key to be flushed, got %s (found=%v)", string(value), found)
	}
	if err := txn.Commit(); err != nil {
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

	if value, found := txn2.Get(x.IntKey(1)); found || len(value) != 0 {
		t.Fatalf("Expected value to be empty, got %s (found=%v)", string(value), found)
	}

	if err := txn2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func BenchmarkTxnSet(b *testing.B) {
	dir := b.TempDir()
	// db := froopydb.NewDB(froopydb.DefaultConfig(dir))
	db := froopydb.NewDB(
		&froopydb.DBConfig{
			Folder:          dir,
			MemTableMaxSize: memTableMaxSize,
			ClearOnStart:    true,
			LogLevel:        logger.INFO,
		},
	)
	defer db.Close()

	b.ResetTimer()
	txn := db.NewTransaction()
	for i := 0; i < b.N; i++ {
		txn.Set(x.IntKey(i), []byte("load"))
		if i%1000 == 0 {
			txn.Commit()
			txn = db.NewTransaction()
		}
	}
	b.StopTimer()
}

func BenchmarkTxnGet(b *testing.B) {
	dir := b.TempDir()
	// db := froopydb.NewDB(froopydb.DefaultConfig(dir))
	db := froopydb.NewDB(
		&froopydb.DBConfig{
			Folder:          dir,
			MemTableMaxSize: memTableMaxSize,
			ClearOnStart:    true,
			LogLevel:        logger.INFO,
		},
	)
	defer db.Close()

	txn := db.NewTransaction()

	// Populate the database
	for i := 0; i < b.N; i++ {
		txn.Set(x.IntKey(i), []byte("load"))
	}
	txn.Commit()
	// db.WaitJobs()

	b.ResetTimer()
	txn = db.NewTransaction()
	for i := 0; i < b.N; i++ {
		txn.Get(x.IntKey(i))
	}
	txn.Commit()
	b.StopTimer()
}
