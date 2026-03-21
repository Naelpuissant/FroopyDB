package table_test

import (
	"froopydb/logger"
	"froopydb/table"
	"testing"
)

func TestSSTableStoreAddNew(t *testing.T) {
	logger := logger.NewLogger(logger.DEBUG)

	dir := t.TempDir()
	store, _ := table.NewSSTableStore(logger, dir)

	sst1 := store.AddNew()
	if sst1.Name() != dir+"/0_0.sst" {
		t.Fatalf("expected name %s/0_1.sst, got %s", dir, sst1.Name())
	}

	sst2 := store.AddNew()
	if sst2.Name() != dir+"/0_1.sst" {
		t.Fatalf("expected name %s/0_2.sst, got %s", dir, sst2.Name())
	}
}
