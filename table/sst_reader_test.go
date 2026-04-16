package table_test

import (
	"bytes"
	"froopydb/table"
	"froopydb/x"
	"os"
	"testing"
)

func createTestSSTable(dir string) *os.File {
	newTable := table.NewSSTable(dir, 0, 0, false, 0)
	_, err := newTable.Open()
	if err != nil {
		panic(err)
	}

	newTable.InitWriter()

	newTable.WriteDataBlock(x.EncodeKey([]byte("key1"), 1), []byte("value1"))
	newTable.WriteDataBlock(x.EncodeKey([]byte("key1"), 2), []byte("value1.2"))
	newTable.WriteDataBlock(x.EncodeKey([]byte("key1"), 3), []byte("value1.3"))

	newTable.WriteDataBlock(x.EncodeKey([]byte("key2"), 1), []byte("value2"))
	newTable.WriteDataBlock(x.EncodeKey([]byte("key2"), 2), []byte{0x00}) // Deleted

	newTable.WriteDataBlock(x.EncodeKey([]byte("key3"), 1), []byte("value3"))
	newTable.WriteDataBlock(x.EncodeKey([]byte("key3"), 2), []byte("value3.2"))

	newTable.WriteDataBlock(x.EncodeKey([]byte("key4"), 1), []byte("value4"))
	newTable.WriteDataBlock(x.EncodeKey([]byte("key5"), 1), []byte("value5"))

	newTable.WriteDataBlock(x.EncodeKey([]byte("key6"), 1), []byte("value6"))
	newTable.WriteDataBlock(x.EncodeKey([]byte("key6"), 2), []byte("value6.2"))

	idxOffset, _ := newTable.WriteIndex()
	bfOffset, _ := newTable.WriteBloomFilter()
	newTable.WriteMetadata(idxOffset, bfOffset)
	newTable.FlushWriter()
	newTable.Ready()

	return newTable.File()
}

func TestSSTableReaderIndex(t *testing.T) {
	file := createTestSSTable(t.TempDir())
	sstReader, err := table.NewSSTReader(file)
	if err != nil {
		t.Fatalf("failed to create SSTReader: %v", err)
	}

	if sstReader.Metadata.Level != 0 {
		t.Fatalf("expected level 0, got %d", sstReader.Metadata.Level)
	}
	if sstReader.Metadata.Incr != 0 {
		t.Fatalf("expected incr 0, got %d", sstReader.Metadata.Incr)
	}
	if sstReader.Metadata.NKeys != 11 {
		t.Fatalf("expected nkeys 11, got %d", sstReader.Metadata.NKeys)
	}
	if sstReader.Metadata.IdxOffset != 91 {
		t.Fatalf("expected idx offset 91, got %d", sstReader.Metadata.IdxOffset)
	}
	if sstReader.Metadata.BfOffset != 333 {
		t.Fatalf("expected bf offset 333, got %d", sstReader.Metadata.BfOffset)
	}

	for item, err := range sstReader.IndexIter() {
		k, ts := x.DecodeKey(item.Key)
		println(string(k), ts, item.Offset)
		if err != nil {
			t.Fatalf("failed to iterate index: %v", err)
		}
		_, err := sstReader.ReadValueAtOffset(int64(item.Offset))
		if err != nil {
			t.Fatalf("failed to read value at offset %d: %v", item.Offset, err)
		}
	}

	// Search key with ts being after last record
	idxItem, found := sstReader.Search(x.EncodeKey([]byte("key1"), 4))
	if !found {
		t.Fatalf("expected to find key, but not found")
	}
	expected := x.EncodeKey([]byte("key1"), 3)
	if !bytes.Equal(idxItem.Key, expected) {
		t.Fatalf("expected key %v, got %v", expected, idxItem.Key)
	}

	// Search key deleted key
	idxItem, found = sstReader.Search(x.EncodeKey([]byte("key2"), 2))
	if !found {
		t.Fatalf("expected to find deleted key, but not found")
	}

	// Search key with ts being before first record
	idxItem, found = sstReader.Search(x.EncodeKey([]byte("key3"), 0))
	if found {
		t.Fatalf("expected to not find key, but found")
	}

	// Search non existing key
	_, found = sstReader.Search(x.EncodeKey([]byte("keyX"), 1))
	if found {
		t.Fatalf("expected not to find non existing key, but found")
	}

	// Search for last record of key6
	idxItem, found = sstReader.Search(x.EncodeKey([]byte("key6"), 42))
	if !found {
		t.Fatalf("expected to find key, but not found")
	}
	expected = x.EncodeKey([]byte("key6"), 2)
	if !bytes.Equal(idxItem.Key, expected) {
		t.Fatalf("expected key %v, got %v", expected, idxItem.Key)
	}
}
