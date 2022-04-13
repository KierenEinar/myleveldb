package memdb

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	maxLevel = 0x12
	rate     = 1 >> 2
)

const (
	nKV = iota
	nKLen
	nVLen
	nLevel
	nNext
)

// DB 内存db, 采用skiplist实现
type DB struct {
	rwMutex sync.RWMutex
	rnd     *rand.Rand
	kvData  []byte // 存放key, value
	// 代表的是每个node节点
	// nodeData[0] => 当前节点对应的kvData的offset
	// nodeData[1] => klen
	// nodeData[2] => vlen
	// nodeData[3] => level 该节点的level
	// nodeData[4] - nodeData[4+level] => 代表每一层的forward节点(0-level)
	nodeData []int
	level    int //当前skiplist层级
	prevNode [maxLevel]int
	n        int // node的数量
	kvSize   int // size
}

// 获取头部
func (db *DB) head() int {
	return nKV
}

func (db *DB) randLevel() int {
	level := 1
	for level < maxLevel && (db.rnd.Uint32()&0xffff) < (0xffff*rate) {
		level++
	}
	return level
}

// Capacity 返回skiplist的容量
func (db *DB) Capacity() int {
	db.rwMutex.RLock()
	defer db.rwMutex.RUnlock()
	return cap(db.kvData)
}

// Length 返回skiplist的节点长度
func (db *DB) Length() int {
	db.rwMutex.RLock()
	defer db.rwMutex.RUnlock()
	return db.n
}

// Size 返回大致的size, delete操作会使该值减小, 但是实际的kvData不会变小
func (db *DB) Size() int {
	db.rwMutex.RLock()
	defer db.rwMutex.RUnlock()
	return db.kvSize
}

// Free 返回剩余空间
func (db *DB) Free() int {
	db.rwMutex.RLock()
	defer db.rwMutex.RUnlock()
	return cap(db.kvData) - len(db.kvData)
}

// 找出大于等于key的节点, 如果prev为true, 会把db.preNode都填充, 每层为小于key的最大值
func (db *DB) findGreaterOrEqual(key []byte, prev bool) (int, bool) {
	node := 0
	level := db.level
	for i := level - 1; i >= 0; i-- {
		next := db.nodeData[node+nNext+level]
		// 如果next节点仍然比key小, 那么继续遍历
		for next != 0 && bytes.Compare(db.nextKey(next, i), key) < 0 {
			next = next + nNext + level
		}
		node = next
		// 如果需要prevnode, 则填充
		if prev {
			db.prevNode[i] = node
			continue
		}
	}

	if db.hasNext(node, 0) {
		next := db.next(node, 0)
		return next, bytes.Compare(db.key(next), key) == 0
	}
	return 0, false
}

// 根据nodeData中的offset获取key
func (db *DB) key(nodeDataOffset int) []byte {
	kLen := db.nodeData[nodeDataOffset+nKLen]
	return db.kvData[nodeDataOffset : nodeDataOffset+kLen]
}

// 根据nodeData中的offset获取value
func (db *DB) value(nodeDataOffset int) []byte {
	kLen := db.nodeData[nodeDataOffset+nKLen]
	o := nodeDataOffset + kLen
	vLen := db.nodeData[nodeDataOffset+nVLen]
	return db.kvData[o : o+vLen]
}

//	返回nodeData当前level的下一个key
func (db *DB) nextKey(nodeDataOffset int, level int) []byte {
	return db.key(nodeDataOffset + nNext + level)
}

// 返回nodeData当前level的下一个value
func (db *DB) nextValue(nodeDataOffset int, level int) []byte {
	return db.value(nodeDataOffset + nNext + level)
}

// 当前节点所在的层的下一个节点
func (db *DB) next(nodeDataOffset, level int) int {
	return db.nodeData[nodeDataOffset+nNext+level]
}

// 当前节点所在的层是否有下一个节点
func (db *DB) hasNext(nodeDataOffset, level int) bool {
	return db.next(nodeDataOffset, level) != 0
}

// Put 将key, value放入到db中
func (db *DB) Put(key, value []byte) {
	db.rwMutex.Lock()
	defer db.rwMutex.Unlock()

	if node, exact := db.findGreaterOrEqual(key, true); exact {
		kvOffset := len(db.kvData)
		db.kvData = append(db.kvData, key...)
		db.kvData = append(db.kvData, value...)
		db.kvSize += len(value) - db.nodeData[node+nVLen]
		db.nodeData[node] = kvOffset
		db.nodeData[node+nKLen] = len(key)
		db.nodeData[node+nVLen] = len(value)
		return
	}

	level := db.randLevel()
	if level > db.level {
		for i := db.level; i < level; i++ {
			db.prevNode[i] = 0
		}
		db.level = level
	}

	kvOffset := len(db.kvData)
	db.kvData = append(db.kvData, key...)
	db.kvData = append(db.kvData, value...)
	db.nodeData = append(db.nodeData, kvOffset, len(key), len(value), level)

	// 自底向上插入
	for i := 0; i < level; i++ {
		prev := db.prevNode[i]
		next := db.next(prev, i)
		db.nodeData[prev+nNext+i] = kvOffset
		db.nodeData = append(db.nodeData, next)
	}

	db.kvSize += len(key) + len(value)
	db.n++
}

// Delete 删除key, kvsize会修改, 但是kvdata不会被裁剪
func (db *DB) Delete(key []byte) bool {

	db.rwMutex.Lock()
	defer db.rwMutex.Unlock()

	node, exact := db.findGreaterOrEqual(key, true)
	if !exact {
		return false
	}

	level := db.nodeData[node+nLevel]
	for i := 0; i < level; i++ {
		prevNode := db.prevNode[i]
		db.nodeData[prevNode+nNext+i] = db.nodeData[node+nNext+i]
	}

	head := db.head()
	for i := db.level; i >= 0; i-- {
		if db.next(head, i) == 0 {
			db.level--
		}
	}
	db.kvSize = db.kvSize - len(db.key(node)) - len(db.value(node))
	db.n--
	return true
}

// Contains 是否存在该key
func (db *DB) Contains(key []byte) bool {
	if _, exact := db.findGreaterOrEqual(key, false); exact {
		return true
	}
	return false
}

// Get 查询key -> value
func (db *DB) Get(key []byte) ([]byte, bool) {
	if node, exact := db.findGreaterOrEqual(key, false); exact {
		return db.value(node), true
	}
	return nil, false
}
