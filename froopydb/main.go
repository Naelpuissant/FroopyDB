package main

import (
	server "froopydb/tcp"
	"log"
)

func main() {
	if err := server.Start(":8080", "/tmp/froopydbserv"); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
