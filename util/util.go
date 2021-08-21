package util

import (
	"bytes"
	"strings"
)

func GetStringSizeInBytes(str string) int32 {
	var countBuf bytes.Buffer
	countBuf.WriteString(str)
	return int32(countBuf.Len())
}

func FindIndex(s, substr string, offset int) int {
	if len(s) < offset {
		return -1
	}
	if idx := strings.Index(s[offset:], substr); idx >= 0 {
		return offset + idx
	}
	return -1
}
