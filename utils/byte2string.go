package utils

import "unsafe"

// BytesToString 超快的字节数组转字符串
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
