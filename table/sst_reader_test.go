package table_test

// TODO : Get a proper test dataset and enable this test

// import (
// 	"froopydb/table"
// 	"os"
// 	"testing"
// )

// func TestSSTableReaderIndex(t *testing.T) {
// 	file, err := os.Open("../test/dataset/0_0.sst")
// 	if err != nil {
// 		t.Fatalf("failed to open sstable: %v", err)
// 	}
// 	defer file.Close()

// 	sstReader, err := table.NewSSTReader(file)
// 	if err != nil {
// 		t.Fatalf("failed to create SSTReader: %v", err)
// 	}

// 	if sstReader.Metadata.Level != 0 {
// 		t.Fatalf("expected level 0, got %d", sstReader.Metadata.Level)
// 	}
// 	if sstReader.Metadata.Incr != 0 {
// 		t.Fatalf("expected incr 0, got %d", sstReader.Metadata.Incr)
// 	}
// 	if sstReader.Metadata.IdxOffset != 0 {
// 		t.Fatalf("expected idx offset 0, got %d", sstReader.Metadata.IdxOffset)
// 	}

// 	for item, err := range sstReader.Index() {
// 		if err != nil {
// 			t.Fatalf("failed to iterate index: %v", err)
// 		}
// 		println(string(item.Key), item.Offset)
// 	}
// }
