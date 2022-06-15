package collections

import (
	"sync/atomic"
	"unsafe"
)

// ConcurrentQueue lock free的队列
type ConcurrentQueue struct {
	head     unsafe.Pointer
	tail     unsafe.Pointer
	capacity int64
}

// NewConcurrentQueue 实例化一个无所队列
func NewConcurrentQueue() *ConcurrentQueue {

	node := &Node{}

	q := &ConcurrentQueue{
		head: unsafe.Pointer(node),
		tail: unsafe.Pointer(node),
	}

	return q
}

// Value ...
type Value interface{}

// Node 队列的节点
type Node struct {
	value Value
	hash  int32
	next  unsafe.Pointer
}

// Offer 添加元素
func (queue *ConcurrentQueue) Offer(hash int32, value Value) {

	node := &Node{
		value: value,
		hash:  hash,
	}

restart:

	tail := loadNode(&queue.tail)
	next := loadNode(&tail.next)

	if tail == loadNode(&queue.tail) {
		// 说明当前tail有可能是真正的尾节点
		if next == nil {
			if cas(&tail.next, nil, node) { // 如果cas成功了, 说明确实是加到队尾了
				cas(&queue.tail, tail, node)
				atomic.AddInt64(&queue.capacity, 1)
				return
			}
		} else {
			// cas如果失败了, 说明加入队尾失败了, 存在hop操作, 那么更新一下tail, 失败也无所谓
			cas(&queue.tail, tail, next)
		}
	}

	goto restart
}

// Pop 弹出队头元素
func (queue *ConcurrentQueue) Pop() Value {

restart:

	head := loadNode(&queue.head)
	tail := loadNode(&queue.tail)
	next := loadNode(&head.next)

	if head == loadNode(&queue.head) {
		// 需要先判断一下是否队列为空的情况
		if tail == head {
			// 说明队列确实是空的, 只有next才是正确反映当时情况
			if next == nil {
				return nil
			}
			// 更新一下tail
			cas(&queue.tail, tail, next)
		} else {

			// 说明队列不为空, 那么取next
			// cas设置head
			v := next.value
			if cas(&queue.head, head, next) {
				atomic.AddInt64(&queue.capacity, -1)
				return v
			}
		}
	}

	goto restart

}

func (queue *ConcurrentQueue) Cap() int64 {
	return atomic.LoadInt64(&queue.capacity)
}

func loadNode(ptr *unsafe.Pointer) *Node {
	return (*Node)(atomic.LoadPointer(ptr))
}

func cas(addr *unsafe.Pointer, oldN *Node, newN *Node) bool {
	return atomic.CompareAndSwapPointer(addr, unsafe.Pointer(oldN), unsafe.Pointer(newN))
}
