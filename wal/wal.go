package wal

import (
	"bytes"
	"fmt"
	"froopydb/x"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type WAL struct {
	folder  string
	file    *os.File
	writeCh chan []byte
}

func openLogFile(folder string, tryRecover bool) *os.File {
	if tryRecover {
		dir, _ := os.ReadDir(folder)

		for _, entry := range dir {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
				file, _ := os.OpenFile(filepath.Join(folder, entry.Name()), os.O_APPEND|os.O_RDWR, 0777)
				return file
			}
		}
	}

	now := time.Now().UnixMilli()
	filename := fmt.Sprintf("%d_%d.log", now, rand.Intn(10000))
	path := filepath.Join(folder, filename)
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)

	return file
}

func NewWAL(folder string, tryRecover bool) *WAL {
	file := openLogFile(folder, tryRecover)
	wal := &WAL{
		folder:  folder,
		file:    file,
		writeCh: make(chan []byte),
	}
	go wal.writer()
	return wal
}

func (w *WAL) writer() {
	for record := range w.writeCh {
		w.file.Write(record)
	}
}

func (w *WAL) Write(key, value []byte) {
	klen := x.Uint16ToBytes(uint16(len(key)))
	vlen := x.Uint16ToBytes(uint16(len(value)))

	var buf bytes.Buffer
	buf.Write(klen)
	buf.Write(vlen)
	buf.Write(key)
	buf.Write(value)

	w.writeCh <- buf.Bytes()
}

// Close and remove log file
func (w *WAL) Finish() {
	w.file.Close()
	os.Remove(w.file.Name() + ".imm")
}

// Mark log file as immutable (add `.imm` prefix)
func (w *WAL) Immutable() {
	os.Rename(w.file.Name(), w.file.Name()+".imm")
}

func (w *WAL) GetFileSize() int64 {
	fs, _ := w.file.Stat()
	return fs.Size()
}

func (w *WAL) File() *os.File {
	return w.file
}
