package table_test

import (
	"froopydb/logger"
	"froopydb/table"
	"froopydb/wal"
	"froopydb/x"
	"testing"
)

func TestFlushMemtable(t *testing.T) {
	logger := logger.NewLogger(logger.DEBUG)
	wal := wal.NewWAL("/tmp", false)
	memTable := table.NewMemTable(logger, 1337, wal)
	memTable.Set(x.Uint32ToBytes(uint32(1)), x.StrToBytes("foo"))
	memTable.Set(x.Uint32ToBytes(uint32(2)), x.StrToBytes("bar"))

	sst := table.NewSSTable("/tmp", 0, 1, true, 0)
	sst.Open()
	defer sst.Close()
	memTable.Flush(sst)
	sst.Ready()

	value1, _ := sst.Search(([4]byte)(x.Uint32ToBytes(uint32(1))))
	if string(value1) != "foo" {
		t.Fatalf("expected 'foo', got %q", value1)
	}
}
