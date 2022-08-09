package iter

import (
	"bytes"
	"myleveldb/collections"
	"myleveldb/utils"
)

// MergedIterator 多路归并排序的迭代器
type MergedIterator struct {
	index int // 需要去哪个iter取数据
	iters []Iterator
	keys  [][]byte
	heap  *collections.Heap
	utils.BasicReleaser
	// soi 代表是否开始遍历
	// eoi 代表是否结束遍历
	soi, eoi bool
	reverse  bool // 如果true, heap为最大堆, 反之为最小堆
	released bool
}

func (mi *MergedIterator) Next() bool {

	if mi.released {
		return false
	}

	if mi.eoi {
		return false
	}

	if mi.soi {
		return mi.First()
	}

	iter := mi.iters[mi.index]
	switch {
	case iter.Next():
		mi.keys[mi.index] = iter.Key()
		mi.heap.Push(mi.index)
	default:
		mi.keys[mi.index] = nil // 这个iterator已经被掏空了
	}

	return mi.next()
}

func (mi *MergedIterator) Seek(key []byte) bool {

	if mi.released {
		return false
	}

	mi.soi = false
	mi.eoi = false

	for x, iter := range mi.iters {
		switch {
		case iter.Seek(key):
			mi.keys[x] = assertKey(iter.Key())
			mi.heap.Push(x)
		default:
			mi.keys[x] = nil
		}
	}

	return mi.next()
}

func (mi *MergedIterator) Key() []byte {
	if mi.eoi || mi.soi {
		return nil
	}
	return mi.keys[mi.index]
}

func (mi *MergedIterator) Value() []byte {
	if mi.eoi || mi.soi {
		return nil
	}
	return mi.iters[mi.index].Value()
}

func (mi *MergedIterator) First() bool {

	if mi.released {
		return false
	}

	for x, iter := range mi.iters {
		switch {
		case iter.First():
			mi.keys[x] = assertKey(iter.Key())
			mi.heap.Push(x)
		default:
			mi.keys[x] = nil
		}
	}

	mi.soi = true

	return mi.next()
}

func (mi *MergedIterator) next() bool {

	if mi.heap.Empty() || mi.eoi {
		if !mi.eoi {
			mi.eoi = true
		}
		return false
	}

	if mi.soi {
		mi.soi = false
	}

	mi.index = mi.heap.Pop().(int)

	return true

}

func (mi *MergedIterator) heapLess(array *[]interface{}, i, j int) bool {
	iIndex, ok := (*array)[i].(int)
	if !ok {
		panic("max heap arr item convert int failed")
	}
	jIndex, ok := (*array)[j].(int)
	if !ok {
		panic("max heap arr item convert int failed")
	}

	r := bytes.Compare(mi.keys[iIndex], mi.keys[jIndex])
	if mi.reverse { // 最大堆
		if r > 0 {
			return true
		}
		return false
	}

	// 最小堆
	if r > 0 {
		return false
	}
	return true

}

func (mi *MergedIterator) UnRef() {

	if !mi.Released() {
		for _, x := range mi.iters {
			x.UnRef()
		}
		mi.heap.Clear()
		mi.keys = nil
		mi.iters = nil
		mi.index = 0
		mi.BasicReleaser.UnRef()
	}

}

func NewMergedIterator(iters []Iterator) Iterator {
	iter := &MergedIterator{
		iters: iters,
		soi:   true,
		keys:  make([][]byte, len(iters)),
	}
	iter.heap = collections.InitHeap(iter.heapLess)
	return iter
}

func assertKey(key []byte) []byte {
	if key == nil {
		panic("myLeveldb/iterator: nil key")
	}
	return key
}
