package x

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

func Uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func StrToBytes(s string) []byte {
	return []byte(s)
}

func BytesToUint16(b []byte) uint16 {
	return binary.LittleEndian.Uint16(b)
}

func BytesToUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func BytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func IntKey(i int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func EncodeKey(key []byte, ts uint64) []byte {
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, ts)
	return append(key, tsBytes...)
}

func DecodeKey(encodedKey []byte) ([]byte, uint64) {
	if len(encodedKey) < 8 {
		return nil, 0
	}
	key := encodedKey[:len(encodedKey)-8]
	tsBytes := encodedKey[len(encodedKey)-8:]
	ts := binary.BigEndian.Uint64(tsBytes)
	return key, ts
}
