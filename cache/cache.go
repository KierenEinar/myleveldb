package cache

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	mInitialSize           = 1 << 4
	mOverflowThreshold     = 1 << 5 // 一个bucket超过这个数量时, 需要overflow++
	mOverflowGrowThreshold = 1 << 7 // 当table的overflow超过这个数量时, 需要扩容
)

type Cacher interface {

	// Capacity 获取容量
	Capacity() int64

	// SetCapacity 设置容量
	SetCapacity(capacity int64)

	// Promote 将node挂载到cacher中, 放在lru的最前面
	Promote(node *Node)

	// Ban 将hash表节点对应的LRUNode从链表中删除，并“尝试”从哈希表中删除数据。
	Ban(node *Node)

	Evict(node *Node)

	EvictNS(ns int64)

	EvictAll()

	// Close
	Close() error
}

type Value interface{}

// Node HashMap 节点
type Node struct {
	d         *Dict
	mu        sync.Mutex
	ns, key   int64
	hash      uint32
	ref       int32
	size      int
	value     Value
	onDel     []func()
	CacheData unsafe.Pointer
}

// Bucket 桶
type Bucket struct {
	mu     sync.Mutex
	nodes  []*Node
	frozen bool
}

// Table 表
type Table struct {
	buckets          []unsafe.Pointer // []*Bucket
	resizeInProgress int32            // 是否正在调整中
	mask             int32
	pred             unsafe.Pointer // 需要被复制的table

	overflow        int32
	growThreshold   int32 // 扩容上限
	shrinkThreshold int32 // 缩容下限

}

// Dict 内存操作
type Dict struct {
	mu     sync.RWMutex
	table  unsafe.Pointer // *Table
	nodes  int32          // 节点数量
	size   int64          // 总容量
	Closed bool
	cacher Cacher
}

// 桶里边查询节点, 不存在则根据noset新建一个node
func (b *Bucket) get(dict *Dict, table *Table, ns, key int64, hash uint32, noset bool) (done bool, added bool, n *Node) {

	b.mu.Lock()

	// 如果是在冻结下, 说明当前bucket不能用了, 直接返回
	if b.frozen {
		b.mu.Unlock()
		return
	}

	for _, node := range b.nodes {
		if node.ns == ns && node.key == key && node.hash == hash {
			atomic.AddInt32(&node.ref, 1)
			b.mu.Unlock()
			return true, false, node
		}
	}

	if noset {
		b.mu.Unlock()
		return true, false, nil
	}

	node := &Node{
		ns:   ns,
		key:  key,
		hash: hash,
		ref:  1,
	}

	b.nodes = append(b.nodes, node)
	blen := len(b.nodes)
	b.mu.Unlock()

	// 是否要扩容
	grow := atomic.AddInt32(&dict.nodes, 1) >= table.growThreshold
	if blen >= mOverflowThreshold {
		grow = grow || atomic.AddInt32(&table.overflow, 1) >= mOverflowGrowThreshold
	}

	if grow && atomic.CompareAndSwapInt32(&table.resizeInProgress, 0, 1) {
		nlen := len(table.buckets) << 1
		nTable := &Table{
			buckets:         make([]unsafe.Pointer, nlen),
			mask:            int32(nlen - 1),
			pred:            unsafe.Pointer(table),
			growThreshold:   int32(nlen * mOverflowThreshold),
			shrinkThreshold: int32(nlen >> 1),
		}
		if !atomic.CompareAndSwapPointer(&dict.table, unsafe.Pointer(table), unsafe.Pointer(nTable)) {
			panic("Bucket get bug")
		}

		go nTable.loadBuckets()

	}

	return true, true, node

}

// 删除节点
func (b *Bucket) delete(dict *Dict, table *Table, ns, key int64, hash uint32) (done bool, deleted bool) {

	b.mu.Lock()
	if b.frozen {
		b.mu.Unlock()
		return
	}

	var (
		node *Node
		bLen int
	)

	for i := range b.nodes {
		node = b.nodes[i]
		if node.ns == ns && node.key == key {
			if atomic.LoadInt32(&node.ref) == 0 {
				deleted = true
				node.value = nil
				b.nodes = append(b.nodes[:i], b.nodes[i+1:]...)
				bLen = len(b.nodes)
			}
			break
		}
	}
	b.mu.Unlock()
	if deleted {

		for _, f := range node.onDel {
			f()
		}

		atomic.AddInt64(&dict.size, int64(node.size*-1))
		shrink := atomic.AddInt32(&dict.nodes, -1) < table.shrinkThreshold

		if bLen >= mOverflowThreshold {
			atomic.AddInt32(&table.overflow, -1)
		}
		//进入缩容流程
		if shrink && len(table.buckets) > mInitialSize && atomic.CompareAndSwapInt32(&table.resizeInProgress, 0, 1) {
			nlen := len(table.buckets) >> 1
			nTable := &Table{
				buckets:         make([]unsafe.Pointer, nlen),
				mask:            int32(nlen - 1),
				pred:            unsafe.Pointer(table),
				growThreshold:   int32(nlen * mOverflowThreshold),
				shrinkThreshold: int32(nlen >> 1),
			}
			ok := atomic.CompareAndSwapPointer(&dict.table, unsafe.Pointer(table), unsafe.Pointer(nTable))

			if !ok {
				panic("bucket delete bug")
			}

			go nTable.loadBuckets()

		}

	}

	return true, deleted

}

func (table *Table) loadBuckets() {
	for i := range table.buckets {
		table.loadBucket(int32(i))
	}
	atomic.StorePointer(&table.pred, nil)
}

func (table *Table) loadBucket(i int32) *Bucket {

	if bucket := (*Bucket)(atomic.LoadPointer(&table.buckets[i])); bucket != nil {
		return bucket
	}

	oldTable := (*Table)(atomic.LoadPointer(&table.pred))

	if oldTable != nil {
		var nodes []*Node
		// 说明是扩容
		if table.mask > oldTable.mask {
			p := (*Bucket)(atomic.LoadPointer(&oldTable.buckets[i&oldTable.mask]))
			if p == nil {
				p = oldTable.loadBucket(i & oldTable.mask)
			}

			m := p.freeze()
			for _, node := range m {
				if int32(node.hash)&table.mask == i {
					nodes = append(nodes, node)
				}
			}
		} else { // 说明是缩容

			p0 := (*Bucket)(atomic.LoadPointer(&oldTable.buckets[i]))
			if p0 == nil {
				p0 = oldTable.loadBucket(i)
			}

			p1 := (*Bucket)(atomic.LoadPointer(&oldTable.buckets[int(i)+len(oldTable.buckets)]))
			if p1 == nil {
				p1 = oldTable.loadBucket(i + int32(len(oldTable.buckets)))
			}

			m0 := p0.freeze()
			m1 := p1.freeze()

			nodes = make([]*Node, 0, len(m0)+len(m1))
			nodes = append(nodes, m0...)
			nodes = append(nodes, m1...)

		}
		nBucket := &Bucket{nodes: nodes}
		if atomic.CompareAndSwapPointer(&table.buckets[i], nil, unsafe.Pointer(nBucket)) {
			if len(nodes) > mOverflowThreshold {
				atomic.AddInt32(&table.overflow, int32(len(nodes)-mOverflowThreshold))
			}
			return nBucket
		}
	}

	// table.pred或者是atomic.compareAndSwap(&buckets[i], nil, newBucket)失败, 说明被其他携程改为nil(已经完成扩/缩容)
	return (*Bucket)(atomic.LoadPointer(&table.buckets[i]))
}

func (b *Bucket) freeze() []*Node {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.frozen {
		b.frozen = true
	}
	return b.nodes
}

func (n *Node) unref() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.d.delete(n)
	}
}

func (n *Node) unrefLocked() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.d.mu.RLock()
		if !n.d.Closed {
			n.d.delete(n)
		}
		n.d.mu.RUnlock()
	}
}

func (dict *Dict) Get(ns, key int64, setFunc func() (size int, value Value)) *Handler {

	dict.mu.RLock()
	defer dict.mu.RUnlock()

	if dict.Closed {
		return nil
	}

	hash := murmur32(ns, key)

	for {

		t, b := dict.getBucket(hash)
		done, _, node := b.get(dict, t, ns, key, hash, setFunc == nil)
		if done {
			if node != nil {
				if node.value == nil {
					node.mu.Lock()
					if setFunc == nil {
						node.mu.Unlock()
						node.unref()
						return nil
					}
					node.size, node.value = setFunc()
					if node.value == nil {
						node.size = 0
						node.mu.Unlock()
						node.unref()
						return nil
					}
					node.mu.Unlock()
					atomic.AddInt64(&dict.size, int64(node.size))
				}

				if dict.cacher != nil {
					dict.cacher.Promote(node)
				}

				return &Handler{n: unsafe.Pointer(node)}

			}
			break
		}
	}
	return nil
}

func (dict *Dict) Delete(ns, key int64, onDel func()) bool {

	dict.mu.RLock()
	defer dict.mu.RUnlock()
	if dict.Closed {
		return false
	}

	hash := murmur32(ns, key)

	for {

		t, b := dict.getBucket(hash)
		done, _, node := b.get(dict, t, ns, key, hash, true)
		if done {
			if node != nil {
				if onDel != nil {
					node.mu.Lock()
					node.onDel = append(node.onDel, onDel)
					node.mu.Unlock()
				}

				if dict.cacher != nil {
					dict.cacher.Ban(node)
				}
				node.unref()
				return true
			}
			break
		}
	}

	if onDel != nil {
		onDel()
	}

	return false
}

// Close 关闭dict
func (dict *Dict) Close() error {
	dict.mu.Lock()
	defer dict.mu.Unlock()
	if !dict.Closed {
		dict.Closed = true
		table := (*Table)(dict.table)
		table.loadBuckets()
		for i := range table.buckets {
			bucket := (*Bucket)(table.buckets[i])
			for _, n := range bucket.nodes {
				if n.value != nil {
					n.value = nil
				}

				for _, f := range n.onDel {
					f()
				}
			}
		}
	}
	return nil
}

func (dict *Dict) getBucket(hash uint32) (*Table, *Bucket) {
	table := (*Table)(atomic.LoadPointer(&dict.table))
	i := int32(hash) & table.mask
	bucket := (*Bucket)(atomic.LoadPointer(&table.buckets[i]))
	if bucket == nil {
		bucket = table.loadBucket(i)
	}
	return table, bucket
}

func (dict *Dict) delete(node *Node) bool {
	h, b := dict.getBucket(node.hash)
	for {
		done, deleted := b.delete(dict, h, node.ns, node.key, node.hash)
		if done {
			return deleted
		}
	}
}

type Handler struct {
	n unsafe.Pointer
}

func (h *Handler) Value() Value {
	n := (*Node)(atomic.LoadPointer(&h.n))
	if n == nil {
		return nil
	}
	return n.value
}

func (h *Handler) Release() {
	nPtr := atomic.LoadPointer(&h.n)
	if nPtr != nil && atomic.CompareAndSwapPointer(&h.n, nPtr, nil) {
		n := (*Node)(nPtr)
		n.unrefLocked()
	}
}

func (n *Node) GetHandler() *Handler {
	atomic.AddInt32(&n.ref, 1)
	return &Handler{n: unsafe.Pointer(n)}
}

func murmur32(ns, key int64) uint32 {
	return 0
}
