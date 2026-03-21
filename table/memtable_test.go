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
	dir := t.TempDir()
	wal := wal.NewWAL(dir, false)
	memTable := table.NewMemTable(logger, 1337, wal)

	memTable.Set(x.EncodeKey(x.IntKey(1), 1), x.StrToBytes("foo"))
	memTable.Set(x.EncodeKey(x.IntKey(2), 1), x.StrToBytes("bar"))

	sst := table.NewSSTable(dir, 0, 1, true, 0)
	sst.Open()
	defer sst.Close()
	err := memTable.Flush(sst)
	if err != nil {
		panic(err)
	}

	value1, _ := sst.Search(x.EncodeKey(x.IntKey(1), 2))
	if string(value1) != "foo" {
		t.Fatalf("expected 'foo', got %q", value1)
	}
}
