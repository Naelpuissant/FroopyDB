package server

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"froopydb"
)

var db *froopydb.DB

// Binary protocol:
// Command: [opcode:1byte][key_len:4bytes][key][value_len:4bytes][value]
// Response: [success:1byte][data_len:4bytes][data]

const (
	OP_SET     byte = 1
	OP_GET     byte = 2
	OP_DELETE  byte = 3
	OP_METRICS byte = 4
)

// HandleConnection processes a single client connection
func HandleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read opcode
		opcode := make([]byte, 1)
		if _, err := conn.Read(opcode); err != nil {
			break
		}

		switch opcode[0] {
		case OP_SET:
			handleSet(conn)

		case OP_GET:
			handleGet(conn)

		case OP_DELETE:
			handleDelete(conn)

		case OP_METRICS:
			handleMetrics(conn)

		default:
			sendResponse(conn, false, nil)
		}
	}
}

func readString(conn net.Conn) (string, error) {
	lenBuf := make([]byte, 4)
	if _, err := conn.Read(lenBuf); err != nil {
		return "", err
	}
	len := binary.LittleEndian.Uint32(lenBuf)

	data := make([]byte, len)
	if _, err := conn.Read(data); err != nil {
		return "", err
	}
	return string(data), nil
}

func sendResponse(conn net.Conn, success bool, data []byte) {
	successByte := byte(0)
	if success {
		successByte = 1
	}
	conn.Write([]byte{successByte})

	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	conn.Write(lenBuf)
	conn.Write(data)
}

func handleSet(conn net.Conn) {
	key, err := readString(conn)
	if err != nil {
		sendResponse(conn, false, nil)
		return
	}

	value, err := readString(conn)
	if err != nil {
		sendResponse(conn, false, nil)
		return
	}

	db.Set([]byte(key), []byte(value))
	sendResponse(conn, true, nil)
}

func handleGet(conn net.Conn) {
	key, err := readString(conn)
	if err != nil {
		sendResponse(conn, false, nil)
		return
	}

	value := db.Get([]byte(key))
	sendResponse(conn, true, []byte(value))
}

func handleDelete(conn net.Conn) {
	key, err := readString(conn)
	if err != nil {
		sendResponse(conn, false, nil)
		return
	}

	db.Delete([]byte(key))
	sendResponse(conn, true, nil)
}

func handleMetrics(conn net.Conn) {
	// For now, send empty metrics response
	// You could serialize metrics to binary if needed
	sendResponse(conn, true, nil)
}

// Start initializes and starts the TCP server
func Start(addr string, dbPath string) error {
	db := froopydb.NewDB(dbPath, 0, 0, false, 1)
	defer db.Close()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("TCP server listening on %s", listener.Addr())

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		log.Println("shutting down server")
		listener.Close()
	}()

	// Handle one connection at a time
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		log.Printf("accepted connection from %s", conn.RemoteAddr())
		HandleConnection(conn)
		log.Printf("closed connection from %s", conn.RemoteAddr())
	}

	log.Println("server stopped")
	return nil
}
