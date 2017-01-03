package storage

import (
	"bytes"
	"encoding/binary"
)

// FloatToBytes converts a float64 to bytes
func FloatToBytes(value float64) []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.LittleEndian, value)
	return buf.Bytes()
}

// BytesToFloat converts a slice of bytes to a float
func BytesToFloat(data []byte) float64 {
	buf := bytes.NewReader(data)
	var result float64
	binary.Read(buf, binary.LittleEndian, &result)
	return result
}
