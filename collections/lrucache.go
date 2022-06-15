/**
无阻塞式的扩缩容map
**/

package collections

import (
	"bytes"
	"errors"
	"hash"
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	mInitBucketSlot = 1 << 4 // 默认一个map初始化16个槽
	mBucketOverflow = 1 << 5 // 默认一个bucket超载的上限
	mLruMapOverflow = 1 << 7 // 默认一个lrumap可以允许全局超载的数量
)

var (
	fnv32Pool = sync.Pool{
		New: func() interface{} {
			return fnv.New32a()
		},
	}
)

var (
	ErrNotFound     = errors.New("err key not found value")
	ErrInvalidValue = errors.New("err invalid value")
	ErrCacheClosed  = errors.New("err cached been closed")
)

type bucketNodeValue struct {
	value    Value
	byteSize int64
}

// BucketNodeDeleterCallback 节点从map中删除会触发的函数回调
type BucketNodeDeleterCallback func(k []byte, value Value)

// bucketNode map bucket的node节点
type bucketNode struct {
	lruMap    *lruMap
	hash      uint32
	namespace uint32
	key       []byte
	value     *bucketNodeValue
	ref       int32
	cacheData unsafe.Pointer            // cacheData的引用
	deleter   BucketNodeDeleterCallback // 当元素被剔除的回调函数
}

// Size 返回节点的内存大小
func (bk *bucketNode) Size() int64 {
	if bk.value == nil {
		return 0
	}
	return bk.value.byteSize
}

// Value 返回节点内容
func (bk *bucketNode) Value() interface{} {
	if bk.value == nil {
		return nil
	}
	return bk.value.value
}

// Unref 减少一次引用次数, 如果变为0了, 则直接回收
func (bk *bucketNode) unref() {
	if atomic.AddInt32(&bk.ref, -1) == 0 {
		bk.lruMap.delete(bk.hash, bk.namespace, bk.key)
	}
}

// Ref 增加一次引用次数
func (bk *bucketNode) Ref() {
	atomic.AddInt32(&bk.ref, 1)
}

// bucket 桶, 相当于map里边的shard
type bucket struct {
	frozen bool         // 当前桶是否被冻结, 用于扩容或者缩容行为
	rwLock sync.RWMutex // 读写锁
	nodes  []*bucketNode
}

func (bucket *bucket) freeze() []*bucketNode {

	bucket.rwLock.Lock()
	defer bucket.rwLock.Unlock()
	if !bucket.frozen {
		bucket.frozen = true
	}
	return bucket.nodes
}

type mBucket struct {
	buckets           []unsafe.Pointer // []bucket 数组的ptr
	inProgressMBucket unsafe.Pointer   // 正在扩缩容的mbucket
	shardMask         uint32
	slots             uint32 // 有xx个槽
	overflow          uint32 // 全局bucket超限叠加的数量
	growThreshold     uint32 // 全局扩容的节点上限
	shrinkThreshold   uint32 // 全局缩容的节点下限
}

type lruMap struct {
	mBucket unsafe.Pointer // []*mBucket 的ptr
	nodes   int32          // 总节点数量
}

func newLruMap() *lruMap {

	buckets := make([]unsafe.Pointer, mInitBucketSlot)
	for idx := range buckets {
		buckets[idx] = unsafe.Pointer(&bucket{})
	}

	mb := &mBucket{
		buckets:         buckets,
		shardMask:       mInitBucketSlot - 1,
		slots:           mInitBucketSlot,
		overflow:        0,
		growThreshold:   mInitBucketSlot * mBucketOverflow,
		shrinkThreshold: mInitBucketSlot * mBucketOverflow,
	}

	return &lruMap{
		mBucket: unsafe.Pointer(mb),
		nodes:   0,
	}
}

func loadMBucket(ptr *unsafe.Pointer) *mBucket {
	return (*mBucket)(atomic.LoadPointer(ptr))
}

func loadBucket(ptr *unsafe.Pointer) *bucket {
	return (*bucket)(atomic.LoadPointer(ptr))
}

func loadLruNode(ptr *unsafe.Pointer) *LRUNode {
	return (*LRUNode)(atomic.LoadPointer(ptr))
}

// 根据hash获取bucket
func (m *mBucket) loadBucket(hash uint32) *bucket {
	slot := hash & m.shardMask
	bucket := loadBucket(&m.buckets[slot])
	// 如果当前bucket不为空, 那么返回
	if bucket != nil {
		return bucket
	}
	// 如果当前bucket为空的话, 那么有可能是正在扩/缩容中, 需要直接
	return m.allocateBucket(slot)
}

func (m *mBucket) allocateBuckets() {
	for slot := uint32(0); slot < m.slots; slot++ {
		m.allocateBucket(slot)
	}
	atomic.StorePointer(&m.inProgressMBucket, nil)
}

// 初始化idx的bucket
func (m *mBucket) allocateBucket(slot uint32) *bucket {

	slot = slot & m.shardMask
	// 如果获取的bucket不为空, 那么直接返回即可
	bkt := loadBucket(&m.buckets[slot])
	if bkt != nil {
		return bkt
	}

	bkt = &bucket{}

	// 获取需要被扩/缩容的mbucket
	pred := loadMBucket(&m.inProgressMBucket)
	if pred != nil {
		pb0 := pred.allocateBucket(slot)

		if pb0 != nil {
			if pred.slots < m.slots { // 说明是扩容行为
				p0 := pb0.freeze()
				for _, x := range p0 {
					if x.hash&m.shardMask == slot {
						bkt.nodes = append(bkt.nodes, x)
					}
				}
			} else { // 说明是缩容行为
				pb1 := pred.allocateBucket(slot + m.slots)
				p0 := pb0.freeze()
				p1 := pb1.freeze()
				bkt.nodes = make([]*bucketNode, 0, len(p0)+len(p1))
				bkt.nodes = append(bkt.nodes, p0...)
				bkt.nodes = append(bkt.nodes, p1...)
			}
		}

		if atomic.CompareAndSwapPointer(&m.buckets[slot], nil, unsafe.Pointer(bkt)) {
			bucketLen := len(bkt.nodes)
			// 计算一个bucket的overflow
			if bucketLen > mBucketOverflow {
				atomic.AddUint32(&m.overflow, uint32(bucketLen-mBucketOverflow))
			}
			return bkt
		}

	}

	return loadBucket(&m.buckets[slot])

}

func (m *lruMap) get(namespace, hash uint32, key []byte, createOnNotExists bool) (added bool, node *bucketNode) {

	for {
		// 获取当前的散列表
		mb := loadMBucket(&m.mBucket)
		// 根据hash获取桶
		bucket := mb.loadBucket(hash)
		// 如果当前的桶在冻结中, 那么需要重试
		bucket.rwLock.RLock()
		if bucket.frozen {
			bucket.rwLock.RUnlock()
			continue
		}

		_, bn := bucket.lookUp(hash, namespace, key)
		bucket.rwLock.RUnlock()
		// 如果不需要判断"不存在也要创建节点的话", 那么不管找不找到节点都返回
		if bn != nil {
			atomic.AddInt32(&bn.ref, 1)
			return false, bn
		}

		// 节点没找到
		if !createOnNotExists {
			return false, nil
		}

		bucket.rwLock.Lock()

		node = &bucketNode{
			lruMap:    m,
			hash:      hash,
			namespace: namespace,
			key:       key,
			ref:       1,
		}

		bucket.nodes = append(bucket.nodes, node)
		bLen := len(bucket.nodes)
		bucket.rwLock.Unlock()

		// 如果加入新的节点, 检查需不需要扩容
		grow := atomic.AddInt32(&m.nodes, 1) > int32(mb.growThreshold)

		if bLen > mBucketOverflow {
			grow = grow || atomic.AddUint32(&mb.overflow, 1) > mLruMapOverflow
		}

		// 如果需要扩容的话
		if grow {
			slots := mb.slots << 1
			pred := unsafe.Pointer(mb)
			growMBucket := &mBucket{
				buckets:           make([]unsafe.Pointer, slots),
				inProgressMBucket: pred,
				shardMask:         slots - 1,
				slots:             slots,
				growThreshold:     slots * mBucketOverflow,
				shrinkThreshold:   mb.growThreshold,
			}
			if atomic.CompareAndSwapPointer(&m.mBucket, pred, unsafe.Pointer(growMBucket)) {
				growMBucket.allocateBuckets()
			}
		}

		return true, node
	}

}

// 删掉map集合中的一个节点
func (m *lruMap) delete(namespace, hash uint32, key []byte) {

	for {
		mb := loadMBucket(&m.mBucket)
		bucket := mb.loadBucket(hash)
		bucket.rwLock.Lock()
		if bucket.frozen {
			bucket.rwLock.Unlock()
			continue
		}

		idx, evicted := bucket.lookUp(hash, namespace, key)
		if evicted != nil {
			bucket.nodes = append(bucket.nodes[:idx], bucket.nodes[idx+1:]...)
		}

		bucket.rwLock.Unlock()

		// 如果达到缩容的情况下, 需要判断是不是达到最低限度了
		if atomic.AddInt32(&m.nodes, -1) < int32(mb.shrinkThreshold) &&
			mb.shrinkThreshold > mInitBucketSlot*mBucketOverflow {

			resizeSlot := mb.slots >> 1

			resizeMb := &mBucket{
				shardMask:         resizeSlot - 1,
				slots:             resizeSlot,
				growThreshold:     resizeSlot * mBucketOverflow,
				shrinkThreshold:   resizeSlot * mBucketOverflow >> 1,
				inProgressMBucket: unsafe.Pointer(mb),
				buckets:           make([]unsafe.Pointer, resizeSlot),
			}

			if atomic.CompareAndSwapPointer(&m.mBucket, unsafe.Pointer(mb), unsafe.Pointer(resizeMb)) {
				go resizeMb.allocateBuckets()
			}

		}

		k, v, deleter := evicted.key, evicted.Value(), evicted.deleter

		if deleter != nil {
			deleter(k, v)
		}

		return
	}

}

func hash32(key []byte) uint32 {
	hash32 := fnv32Pool.Get().(hash.Hash32)
	defer func() {
		hash32.Reset()
		fnv32Pool.Put(hash32)
	}()
	_, _ = hash32.Write(key)
	return hash32.Sum32()
}

// 非线程安全的查询, 外部需要加锁
func (bucket *bucket) lookUp(hash, namespace uint32, key []byte) (int, *bucketNode) {

	for idx, n := range bucket.nodes {
		if n.hash == hash && n.namespace == namespace && bytes.Compare(n.key, key) == 0 {
			return idx, n
		}
	}
	return -1, nil
}

// LRUHandle 真正的存放数据的地方
type LRUHandle struct {
	bucketNode unsafe.Pointer // bucket node的ptr
}

func loadBucketNode(nPtr *unsafe.Pointer) *bucketNode {
	return (*bucketNode)(atomic.LoadPointer(nPtr))
}

// Size 返回节点所占用的内存字节大小
func (h *LRUHandle) Size() int64 {
	nPtr := h.bucketNode
	if nPtr != nil {
		return loadBucketNode(&nPtr).Size()
	}
	return 0
}

// Value 返回节点的value
func (h *LRUHandle) Value() interface{} {
	nPtr := h.bucketNode
	if nPtr != nil {
		return loadBucketNode(&nPtr).Value()
	}
	return nil
}

func (h *LRUHandle) UnRef() {
	// 获取bucketnode的节点
	nPtr := h.bucketNode
	if nPtr != nil && atomic.CompareAndSwapPointer(&h.bucketNode, nPtr, nil) {
		node := loadBucketNode(&nPtr)
		node.unref()
	}
}

// LRUNode lru的节点
type LRUNode struct {
	prev   *LRUNode
	next   *LRUNode
	handle *LRUHandle
}

// Lru lru双向链表
type Lru struct {
	recent LRUNode // 没有实际意义的头节点, next链接最新的node, prev链接最旧的node
}

func newLru() *Lru {
	recent := LRUNode{}
	recent.next = &recent
	recent.prev = &recent
	return &Lru{
		recent: recent,
	}
}

// 将node加入到队列头部
func (lru *Lru) insert(node *LRUNode) {

	if node == &lru.recent {
		panic("insert node is recent")
	}

	recent := lru.recent
	recentNext := recent.next
	recent.next = node
	node.prev = &recent

	node.next = recentNext
	recentNext.prev = node

}

// 将node节点从队列中删除
func (lru *Lru) remove(node *LRUNode) {
	if node == &lru.recent {
		panic("remove node is recent")
	}
	if node.next == nil || node.prev == nil || node.handle == nil {
		panic("myleveldb bug report, node next or prev is nil")
	}

	prev := node.prev
	next := node.next
	prev.next = next
	next.prev = prev
}

// LRUCache lrucache的实现
type LRUCache struct {
	lruMap      *lruMap // 真正存放数据的map
	lru         *Lru    // 双向链表, next存放最新的, prev存放最旧的, 通过双向联表, 控制map的kv哪些需要被驱逐
	capacity    int64   // 最大可允许的节点总容量
	size        int64   // 当前的容量
	mutex       sync.RWMutex
	closedMutex sync.RWMutex
	closed      bool
}

func NewLRUCache(capacity int64) *LRUCache {

	cache := new(LRUCache)
	cache.capacity = capacity
	cache.lru = newLru()
	cache.lruMap = newLruMap()
	runtime.KeepAlive(cache)
	runtime.SetFinalizer(cache, cache.Close())
	return cache
}

// SetFunc 设置value的函数
type SetFunc func() (int64, Value, BucketNodeDeleterCallback, error)

// Get 从map中获取, 如果获取不到的话, sf不为空则会把k,v放置到lru的最新节点上, 并把kv放进map中
// 当从该函数获取LRUHandle时，需要手动进行一次release()函数调用以减少一次引用计数
func (lruCache *LRUCache) Get(namespace uint32, key []byte, f SetFunc) (*LRUHandle, error) {
	lruCache.closedMutex.RLock()
	defer lruCache.closedMutex.RUnlock()
	if lruCache.closed {
		return nil, ErrCacheClosed
	}

	h := hash32(key)
	added, node := lruCache.lruMap.get(namespace, h, key, f != nil)

	if node == nil {
		return nil, ErrNotFound
	}

	handle := &LRUHandle{
		bucketNode: unsafe.Pointer(node),
	}

	// 如果不是添加的节点, 那么说明之前该节点已经存在
	// 需要重新放入到队列中
	if !added {
		lruCache.promote(node)
		return handle, nil
	}

	size, value, deleter, err := f()
	if err != nil {
		node.unref() // 释放掉node
		return nil, err
	}

	if size == 0 || value == nil {
		node.unref()
		return nil, ErrInvalidValue
	}

	node.value = &bucketNodeValue{
		value:    value,
		byteSize: size,
	}
	node.deleter = deleter
	err = lruCache.promote(node)
	if err != nil {
		node.unref()
		return nil, err
	}

	return handle, nil

}

// Delete 从缓存直接删掉
func (lruCache *LRUCache) Delete(ns uint32, key []byte) (bool, error) {

	lruCache.closedMutex.RLock()
	defer lruCache.closedMutex.RUnlock()
	if lruCache.closed {
		return false, ErrCacheClosed
	}

	hash32 := hash32(key)

	for {
		mBucket := loadMBucket(&lruCache.lruMap.mBucket)
		bucket := mBucket.loadBucket(hash32)
		bucket.rwLock.Lock()
		if bucket.frozen {
			bucket.rwLock.Unlock()
			continue
		}

		idx, node := bucket.lookUp(hash32, ns, key)
		if idx == -1 {
			bucket.rwLock.Unlock()
			return false, nil
		}
		bucket.rwLock.Unlock()

		// map自身解除引用
		node.unref()

		nPtr := node.cacheData
		if atomic.CompareAndSwapPointer(&node.cacheData, nPtr, nil) {
			// lru解除引用
			lruNode := loadLruNode(&nPtr)
			lruCache.mutex.Lock()
			lruCache.lru.remove(lruNode)
			lruCache.size -= node.Size()
			lruCache.mutex.Unlock()
			lruNode.handle.UnRef() // 节点解除对node的引用
		}

		return true, nil

	}

}

// Close 设置关闭方法
func (lruCache *LRUCache) Close() error {

	lruCache.closedMutex.Lock()
	defer lruCache.closedMutex.Unlock()
	if !lruCache.closed {
		lruCache.closed = true
		lruCache.mutex.Lock()
		lru := lruCache.lru
		// 倾倒lru链表
		recent := lru.recent
		eldest := recent.prev
		removed := make([]*LRUNode, 0)
		// 从后往前遍历
		for ; eldest != &recent; eldest = eldest.prev {
			lru.remove(eldest)
			// 解除自身对map的引用
			removed = append(removed, eldest)
		}
		lruCache.mutex.Unlock()
		for _, v := range removed {
			v.handle.UnRef()
		}
	}
	return nil
}

// promote 将节点挂载到lru上
func (lruCache *LRUCache) promote(node *bucketNode) error {
	lruNode := (*LRUNode)(atomic.LoadPointer(&node.cacheData))
	lruCache.mutex.Lock()

	if lruCache.closed {
		return ErrCacheClosed
	}

	if lruNode != nil {
		lruCache.lru.remove(lruNode)
		lruCache.lru.insert(lruNode)
		lruCache.mutex.Unlock()
		return nil
	}

	node.Ref()

	lruNode = &LRUNode{
		handle: &LRUHandle{
			bucketNode: unsafe.Pointer(node),
		},
	}

	lruCache.lru.insert(lruNode)
	lruCache.size += node.Size()

	if lruCache.size < lruCache.capacity {
		lruCache.mutex.Unlock()
		return nil
	}
	recent := lruCache.lru.recent
	eldest := recent.prev
	removed := make([]*LRUNode, 0)
	for eldest != &recent && lruCache.size >= lruCache.capacity {
		lruCache.lru.remove(eldest)
		lruCache.size -= eldest.handle.Size()
		removed = append(removed, eldest)
		eldest = recent.prev
	}

	lruCache.mutex.Unlock()

	for _, v := range removed {
		v.handle.UnRef()
	}

	return nil

}
