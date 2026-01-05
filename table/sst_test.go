package table_test

import (
	"froopydb/logger"
	"froopydb/table"
	"testing"
)

func TestNewSSTable(t *testing.T) {
	logger := logger.NewLogger(logger.DEBUG)

	sst := table.NewSSTable(logger, "/tmp", 0, 1, false, 0)
	if sst.Name() != "/tmp/0_1.sst" {
		t.Fatalf("expected name /tmp/0_1.sst, got %s", sst.Name())
	}
	_, err := sst.Open()
	if err != nil {
		t.Fatalf("failed to open SSTable: %v", err)
	}
	defer sst.Close()
}
