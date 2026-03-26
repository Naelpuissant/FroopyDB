package compact_test

import (
	"froopydb/compact"
	"froopydb/logger"
	"froopydb/table"
	"froopydb/x"
	"os"
	"path/filepath"
	"testing"
)

func TestMaybeCompact(t *testing.T) {
	logger := logger.NewLogger(logger.DEBUG)

	dir := t.TempDir()

	store, _ := table.NewSSTableStore(logger, dir)

	// Create 3 SSTables (level 0) to trigger compaction
	t0 := store.AddNew()
	t0.Open()
	t0.InitWriter()
	t0.WriteDataBlock(x.EncodeKey(x.IntKey(1), 0), []byte("A"))
	idxOffset, _ := t0.WriteIndex()
	bfOffset, _ := t0.WriteBloomFilter()
	t0.WriteMetadata(idxOffset, bfOffset)
	t0.FlushWriter()
	t0.Ready()
	defer t0.Close()

	t1 := store.AddNew()
	t1.Open()
	t1.InitWriter()
	t1.WriteDataBlock(x.EncodeKey(x.IntKey(2), 0), []byte("B"))
	idxOffset, _ = t1.WriteIndex()
	bfOffset, _ = t1.WriteBloomFilter()
	t1.WriteMetadata(idxOffset, bfOffset)
	t1.FlushWriter()
	t1.Ready()
	defer t1.Close()

	t2 := store.AddNew()
	t2.Open()
	t2.InitWriter()
	t2.WriteDataBlock(x.EncodeKey(x.IntKey(3), 0), []byte("C"))
	idxOffset, _ = t2.WriteIndex()
	bfOffset, _ = t2.WriteBloomFilter()
	t2.WriteMetadata(idxOffset, bfOffset)
	t2.FlushWriter()
	t2.Ready()
	defer t2.Close()

	if len(store.Tables()[0]) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(store.Tables()))
	}

	compact.MaybeCompact(store)

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
	f, err := os.OpenFile(filepath.Join(dir, newFile), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open new sstable: %v", err)
	}
	newTable, err := table.NewSSTableFromFile(f)
	if err != nil {
		t.Fatalf("failed to load new sstable: %v", err)
	}

	if newTable.Len() != 3 {
		t.Fatalf("expected 3 keys in merged sstable, got %d", newTable.Len())
	}

	tests := map[int]string{
		1: "A",
		2: "B",
		3: "C",
	}

	for key, expected := range tests {
		value, found := newTable.Search(x.EncodeKey(x.IntKey(key), 0))
		if !found || string(value) != expected {
			t.Fatalf("expected %q, got %q for key %v", expected, value, key)
		}
	}
}
