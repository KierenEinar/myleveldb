package memdb

import (
	"myleveldb/collections"
	"myleveldb/comparer"
	"myleveldb/iter"
	"myleveldb/utils"
)

// MemDB 内存kv数据库
type MemDB struct {
	collections.LLRBTree
}

// NewMemDB 新建
func NewMemDB(capacity int, cmp comparer.BasicComparer, pool *utils.BytePool) *MemDB {
	return &MemDB{
		LLRBTree: *collections.NewLLRBTree(capacity, cmp, pool),
	}
}

// NewIterator 新建一个迭代器
func (mdb *MemDB) NewIterator() iter.Iterator {
	return collections.NewLLRBTreeIter(&mdb.LLRBTree, nil)
}
