package comparer

import "bytes"

// BasicComparer 基础的比较器
type BasicComparer interface {
	// Compare 比较
	// 当a元素小于b的时候, 返回-1
	// 相等时返回 0
	// 当a元素大于b的时候, 返回1
	Compare(a, b []byte) int
}

// DefaultComparer 默认比较器
var DefaultComparer = &ByteComparer{}

type ByteComparer struct{}

func (bc ByteComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}
