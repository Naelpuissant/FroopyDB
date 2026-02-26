package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"froopydb"
)

var db *froopydb.DB

type setReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	var req setReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	db.Set([]byte(req.Key), []byte(req.Value))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": req.Key, "value": req.Value})
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	value := db.Get([]byte(key))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": string(value)})
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	db.Delete([]byte(key))
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key})
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	m := db.Metrics()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m)
}

func main() {
	// sensible defaults; adjust as needed
	db = froopydb.NewDB(&froopydb.DBConfig{
		Folder:          "/tmp/froopydbserv",
		MemTableMaxSize: 0,
		ClearOnStart:    false,
		LogLevel:        1,
	})
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /set", setHandler)
	mux.HandleFunc("GET /get", getHandler)
	mux.HandleFunc("DELETE /delete", deleteHandler)
	mux.HandleFunc("GET /metrics", metricsHandler)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// graceful shutdown
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("server stopped")
}
