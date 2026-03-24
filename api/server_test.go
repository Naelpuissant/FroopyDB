package main

import (
	"bytes"
	"encoding/json"
	"froopydb/logger"
	"net/http"
	"net/http/httptest"
	"testing"

	"froopydb"
)

func TestSetGetHandlers(t *testing.T) {
	db = froopydb.NewDB(&froopydb.DBConfig{
		Folder:          t.TempDir(),
		MemTableMaxSize: froopydb.MB,
		ClearOnStart:    false,
		LogLevel:        logger.INFO,
	})
	defer db.Close()

	// Test SET
	body, _ := json.Marshal(map[string]string{"key": "foo", "value": "bar"})
	req := httptest.NewRequest("POST", "/set", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	setHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("setHandler status = %d", rr.Code)
	}

	var out map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("setHandler decode: %v", err)
	}
	if out["value"] != "bar" {
		t.Fatalf("setHandler value = %q; want %q", out["value"], "bar")
	}

	// Test GET
	req = httptest.NewRequest("GET", "/get?key=foo", nil)
	rr = httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("getHandler status = %d", rr.Code)
	}
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("getHandler decode: %v", err)
	}
	if out["value"] != "bar" {
		t.Fatalf("getHandler value = %q; want %q", out["value"], "bar")
	}
}
