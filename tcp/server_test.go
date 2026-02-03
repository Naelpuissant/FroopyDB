package server_test

import (
	"encoding/binary"
	"fmt"
	server "froopydb/tcp"
	"net"
	"testing"

	"froopydb"
)

func sendCommand(conn net.Conn, opcode byte, key string, value string) (bool, string, error) {
	// Send command
	msg := []byte{opcode}
	msg = append(msg, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(msg[1:], uint32(len(key)))
	msg = append(msg, []byte(key)...)
	msg = append(msg, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(msg[len(msg)-4:], uint32(len(value)))
	msg = append(msg, []byte(value)...)

	if _, err := conn.Write(msg); err != nil {
		return false, "", err
	}

	// Read response
	successBuf := make([]byte, 1)
	if _, err := conn.Read(successBuf); err != nil {
		return false, "", err
	}

	lenBuf := make([]byte, 4)
	if _, err := conn.Read(lenBuf); err != nil {
		return false, "", err
	}
	dataLen := binary.LittleEndian.Uint32(lenBuf)

	data := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := conn.Read(data); err != nil {
			return false, "", err
		}
	}

	return successBuf[0] == 1, string(data), nil
}

func TestSetGet(t *testing.T) {
	db := froopydb.NewDB(t.TempDir(), 0, 0, false, 1)
	defer db.Close()

	// Start server in goroutine
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			server.HandleConnection(conn)
		}
	}()

	// Connect to server
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Test SET
	success, _, err := sendCommand(conn, server.OP_SET, "foo", "bar")
	if err != nil || !success {
		t.Fatalf("SET failed: %v", err)
	}

	// Test GET
	success, value, err := sendCommand(conn, server.OP_GET, "foo", "")
	if err != nil || !success {
		t.Fatalf("GET failed: %v", err)
	}
	if value != "bar" {
		t.Fatalf("GET value = %q; want %q", value, "bar")
	}

	// Test DELETE
	success, _, err = sendCommand(conn, server.OP_DELETE, "foo", "")
	if err != nil || !success {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Test GET after DELETE (should return empty)
	success, value, err = sendCommand(conn, server.OP_GET, "foo", "")
	if err != nil || !success {
		t.Fatalf("GET after DELETE failed: %v", err)
	}
	if value != "" {
		t.Fatalf("GET after DELETE value = %q; want empty", value)
	}
}

func BenchmarkSetGetOperations(b *testing.B) {
	db := froopydb.NewDB(b.TempDir(), 0, 0, false, 1)
	defer db.Close()

	// Start server in goroutine that accepts connections
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		b.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.HandleConnection(conn)
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create new connection for each iteration
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}

		key := fmt.Sprintf("k%d", i)
		// SET
		if _, _, err := sendCommand(conn, server.OP_SET, key, "v"); err != nil {
			b.Fatalf("SET failed: %v", err)
		}
		// GET
		if _, _, err := sendCommand(conn, server.OP_GET, key, ""); err != nil {
			b.Fatalf("GET failed: %v", err)
		}
		conn.Close()
	}

	b.StopTimer()
}
