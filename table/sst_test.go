package table_test

import (
	"testing"

	"froopydb/table"
)

func TestNewSSTable(t *testing.T) {
	tmpDir := t.TempDir()
	sst := table.NewSSTable(tmpDir, 0, 1, false, 0)

	if sst.Name() != tmpDir+"/0_1.sst" {
		t.Fatalf("expected name /tmp/0_1.sst, got %s", sst.Name())
	}

	_, err := sst.Open()
	if err != nil {
		t.Fatalf("failed to open SSTable: %v", err)
	}

	err = sst.Close()
	if err != nil {
		t.Fatalf("failed to close SSTable: %v", err)
	}
}
