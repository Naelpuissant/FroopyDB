package table_test

import (
	"froopydb/table"
	"testing"
)

func TestSSTableStoreAddNew(t *testing.T) {
	store, _ := table.NewSSTableStore("/tmp", 100000)

	sst1 := store.AddNew()
	if sst1.Name() != "/tmp/0_1.sst" {
		t.Fatalf("expected name /tmp/0_1.sst, got %s", sst1.Name())
	}

	sst2 := store.AddNew()
	if sst2.Name() != "/tmp/0_2.sst" {
		t.Fatalf("expected name /tmp/0_2.sst, got %s", sst2.Name())
	}
}
