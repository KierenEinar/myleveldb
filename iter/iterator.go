package iter

import "myleveldb/utils"

// CommonIterator 遍历器
type CommonIterator interface {
	First() bool
	Seek(key []byte) bool // 将当前pos移动到一个大于等于key的位置, 并返回是否存在该值
	Next() bool           // 是否存在往后遍历的节点, 每次移动一个节点, 并返回是否还有下一个
	utils.Releaser
	utils.ReleaserSetter
}

type Iterator interface {
	CommonIterator
	Key() []byte   // 获取当前遍历的key
	Value() []byte // 获取当前遍历的value
}
