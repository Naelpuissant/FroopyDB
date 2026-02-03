package table_test

import (
	"froopydb/logger"
	"froopydb/table"
	"testing"
)

func TestSSTableStoreAddNew(t *testing.T) {
	logger := logger.NewLogger(logger.DEBUG)

	store, _ := table.NewSSTableStore(logger, "/tmp")

	sst1 := store.AddNew()
	if sst1.Name() != "/tmp/0_1.sst" {
		t.Fatalf("expected name /tmp/0_1.sst, got %s", sst1.Name())
	}

	sst2 := store.AddNew()
	if sst2.Name() != "/tmp/0_2.sst" {
		t.Fatalf("expected name /tmp/0_2.sst, got %s", sst2.Name())
	}
}
