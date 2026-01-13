package compact_test

import (
	"froopydb/compact"
	"froopydb/logger"
	"froopydb/table"
	"os"
	"path/filepath"
	"testing"
)

func TestMaybeCompactL0(t *testing.T) {
	logger := logger.NewLogger(logger.DEBUG)

	// Create temporary directory
	dir := "/tmp/froopydb/test/compaction_test"

	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)

	store, _ := table.NewSSTableStore(logger, dir, 100000)

	// Create 3 SSTables (level 0) to trigger compaction
	t0 := store.AddNew()
	t0.Open()
	t0.InitWriter()
	t0.WriteBlock([]byte{1}, []byte("A"))
	idxOffset, _ := t0.WriteIndices()
	t0.WriteMetadata(idxOffset)
	t0.FlushWriter()
	defer t0.Close()

	t1 := store.AddNew()
	t1.Open()
	t1.InitWriter()
	t1.WriteBlock([]byte{2}, []byte("B"))
	idxOffset, _ = t1.WriteIndices()
	t1.WriteMetadata(idxOffset)
	t1.FlushWriter()
	defer t1.Close()

	t2 := store.AddNew()
	t2.Open()
	t2.InitWriter()
	t2.WriteBlock([]byte{3}, []byte("C"))
	idxOffset, _ = t2.WriteIndices()
	t2.WriteMetadata(idxOffset)
	t2.FlushWriter()
	defer t2.Close()

	if len(store.Tables()[0]) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(store.Tables()))
	}

	compact.MaybeCompactL0(store)

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
	newTable, err := table.NewSSTableFromFile(f)
	if err != nil {
		t.Fatalf("failed to load new sstable: %v", err)
	}

	if len(newTable.Index()) != 3 {
		t.Fatalf("expected 3 keys in merged sstable, got %d", len(newTable.Index()))
	}

	tests := map[int]string{
		1: "A",
		2: "B",
		3: "C",
	}

	for key, expected := range tests {
		value, err := newTable.Search([]byte{byte(key)})
		if err != nil {
			t.Fatalf("failed to search key %v: %v", key, err)
		}
		if string(value) != expected {
			t.Fatalf("expected %q, got %q for key %v", expected, value, key)
		}
	}
}

func TestMaybeCompactToUpperLevel(t *testing.T) {
	logger := logger.NewLogger(logger.DEBUG)

	// Create temporary directory
	dir := "/tmp/froopydb/test/compaction_test"

	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)

	store, _ := table.NewSSTableStore(logger, dir, 100000)

	t1 := store.AddNew()
	t1.Open()
	t1.InitWriter()
	t1.WriteBlock([]byte{1}, []byte("a"))
	t1.WriteBlock([]byte{4}, []byte("d"))
	idxOffset, _ := t1.WriteIndices()
	t1.WriteMetadata(idxOffset)
	t1.FlushWriter()
	defer t1.Close()

	t2 := store.AddNew()
	t2.Open()
	t2.InitWriter()
	t2.WriteBlock([]byte{4}, []byte("D"))
	idxOffset, _ = t2.WriteIndices()
	t2.WriteMetadata(idxOffset)
	t2.FlushWriter()
	defer t2.Close()

	t3 := store.AddNew()
	t3.Open()
	t3.InitWriter()
	t3.WriteBlock([]byte{1}, []byte("a"))
	idxOffset, _ = t3.WriteIndices()
	t3.WriteMetadata(idxOffset)
	t3.FlushWriter()
	defer t3.Close()

	compact.MaybeCompactL0(store)

	t4 := store.AddNew()
	t4.Open()
	t4.InitWriter()
	t4.WriteBlock([]byte{2}, []byte("B"))
	t4.WriteBlock([]byte{5}, []byte("e"))
	idxOffset, _ = t4.WriteIndices()
	t4.WriteMetadata(idxOffset)
	t4.FlushWriter()
	defer t4.Close()

	t5 := store.AddNew()
	t5.Open()
	t5.InitWriter()
	t5.WriteBlock([]byte{1}, []byte("A"))
	t5.WriteBlock([]byte{3}, []byte("C"))
	t5.WriteBlock([]byte{5}, []byte("E"))
	idxOffset, _ = t5.WriteIndices()
	t5.WriteMetadata(idxOffset)
	t5.FlushWriter()
	defer t5.Close()

	compact.MaybeCompactToUpperLevel(store)

	tests := map[int]string{
		1: "A",
		2: "B",
		3: "C",
		4: "D",
		5: "E",
	}

	for key, expected := range tests {
		value, err := store.Search([]byte{byte(key)})
		if err != nil {
			t.Fatalf("failed to search key %v: %v", key, err)
		}
		if string(value) != expected {
			t.Fatalf("expected %q, got %q for key %v", expected, value, key)
		}
	}
}
