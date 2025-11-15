package src

import (
	"encoding/binary"
)

func Uint16ToBytes(i uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, i)
	return buf
}

func Uint32ToBytes(i uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, i)
	return buf
}

func StrToBytes(s string) []byte {
	return []byte(s)
}

func BytesToUint16(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}
