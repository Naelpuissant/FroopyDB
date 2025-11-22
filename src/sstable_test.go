package src_test

import (
	"froopydb/src"
	"os"
	"path/filepath"
	"testing"
)

func TestMaybeCompactL0(t *testing.T) {
	// Create temporary directory
	dir := "../db/compaction_test"

	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)

	store := src.NewSSTableStore(dir, 100000)

	// Create 3 SSTables (level 0) to trigger compaction

	// SSTable 0
	t0 := store.AddNew()
	t0.Open()
	t0.WriteBlock([4]byte{1, 0, 0, 0}, []byte("A"))
	t0.WriteIndices()
	defer t0.Close()

	// SSTable 1
	t1 := store.AddNew()
	t1.Open()
	t1.WriteBlock([4]byte{2, 0, 0, 0}, []byte("B"))
	t1.WriteIndices()
	defer t1.Close()

	// SSTable 2
	t2 := store.AddNew()
	t2.Open()
	t2.WriteBlock([4]byte{3, 0, 0, 0}, []byte("C"))
	t2.WriteIndices()
	defer t2.Close()

	// Make sure we actually have 3 level-0 SSTables.
	if len(store.Tables()[0]) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(store.Tables()))
	}

	// --- Run compaction ---
	store.MaybeCompactL0()

	// --- Validate compaction result ---

	files, _ := os.ReadDir(dir)
	sstCount := 0
	var newFile string

	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			sstCount++
			newFile = f.Name()
		}
	}

	if sstCount != 1 {
		t.Fatalf("expected 1 sstable after compaction, got %d", sstCount)
	}

	// Open the new merged table
	f, err := os.OpenFile(filepath.Join(dir, newFile), os.O_RDONLY, 0777)
	if err != nil {
		t.Fatalf("failed to open new sstable: %v", err)
	}
	newTable := src.NewSSTableFromFile(f)

	if len(newTable.Index()) != 3 {
		t.Fatalf("expected 3 keys in merged sstable, got %d", len(newTable.Index()))
	}

	tests := map[[4]byte]string{
		{1, 0, 0, 0}: "A",
		{2, 0, 0, 0}: "B",
		{3, 0, 0, 0}: "C",
	}

	for key, expected := range tests {
		value := string(newTable.Search(key))
		if value != expected {
			t.Fatalf("expected %q, got %q for key %v", expected, value, key)
		}
	}
}

func TestMaybeCompactToUpperLevel(t *testing.T) {
	// Create temporary directory
	dir := "../db/compaction_test"

	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)

	store := src.NewSSTableStore(dir, 100000)

	t1 := store.AddNew()
	t1.Open()
	t1.WriteBlock([4]byte{1, 0, 0, 0}, []byte("a"))
	t1.WriteBlock([4]byte{4, 0, 0, 0}, []byte("d"))
	t1.WriteIndices()
	defer t1.Close()

	t2 := store.AddNew()
	t2.Open()
	t2.WriteBlock([4]byte{4, 0, 0, 0}, []byte("D"))
	t2.WriteIndices()
	defer t2.Close()

	t3 := store.AddNew()
	t3.Open()
	t3.WriteBlock([4]byte{1, 0, 0, 0}, []byte("a"))
	t3.WriteIndices()
	defer t3.Close()

	store.MaybeCompactL0()

	t4 := store.AddNew()
	t4.Open()
	t4.WriteBlock([4]byte{2, 0, 0, 0}, []byte("B"))
	t4.WriteBlock([4]byte{5, 0, 0, 0}, []byte("e"))
	t4.WriteIndices()
	defer t4.Close()

	t5 := store.AddNew()
	t5.Open()
	t5.WriteBlock([4]byte{1, 0, 0, 0}, []byte("A"))
	t5.WriteBlock([4]byte{3, 0, 0, 0}, []byte("C"))
	t5.WriteBlock([4]byte{5, 0, 0, 0}, []byte("E"))
	t5.WriteIndices()
	defer t5.Close()

	store.MaybeCompactToUpperLevel()

	tests := map[[4]byte]string{
		{1, 0, 0, 0}: "A",
		{2, 0, 0, 0}: "B",
		{3, 0, 0, 0}: "C",
		{4, 0, 0, 0}: "D",
		{5, 0, 0, 0}: "E",
	}

	for key, expected := range tests {
		value := string(store.Search(key))
		if value != expected {
			t.Fatalf("expected %q, got %q for key %v", expected, value, key)
		}
	}
}
