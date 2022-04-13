package cache

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// lruNode lru节点
type lruNode struct {
	n          *Node
	h          *Handler
	ban        bool
	prev, next *lruNode // next是最旧的, prev是最新的
}

func (node *lruNode) insert(at *lruNode) {
	x := at.prev
	at.prev = node
	node.next = at
	if x != nil {
		node.prev = x
		x.next = node
	}
}

func (node *lruNode) remove() {
	prev := node.prev
	next := node.next
	if prev != nil {
		prev.next = next
		next.prev = prev
	}
}

// lru
type lru struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	recent   lruNode // 头部
}

func (lru *lru) reset() {
	lru.recent.next = &lru.recent
	lru.recent.prev = &lru.recent
	lru.used = 0
}

func (lru *lru) SetCapacity(capacity int64) {
	lru.mu.Lock()
	evicted := make([]*lruNode, 0)
	for lru.used < capacity {
		next := lru.recent.next
		n := (*Node)(atomic.LoadPointer(&next.h.n))
		next.remove()
		lru.used -= int64(n.size)
		evicted = append(evicted, next)
		n.CacheData = nil
	}
	lru.capacity = capacity
	lru.mu.Unlock()
	for _, evict := range evicted {
		evict.h.Release()
	}

}

func (lru *lru) Capacity() int64 {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.capacity
}

func (lru *lru) Promote(n *Node) {
	var evicted []*lruNode
	lru.mu.Lock()
	if n.CacheData == nil {

		if int64(n.size) < lru.capacity {

			ln := &lruNode{n: n, h: n.GetHandler(), ban: false}
			ln.insert(&lru.recent)
			n.CacheData = unsafe.Pointer(ln)
			lru.used += int64(n.size)

			for lru.used > lru.capacity {
				ln := lru.recent.next
				ln.remove()
				n := (*Node)(ln.h.n)
				lru.used -= int64(n.size)
				n.CacheData = nil
				evicted = append(evicted, ln)
			}

		}

	} else {
		ln := (*lruNode)(n.CacheData)
		if !ln.ban {
			ln.remove()
			ln.insert(&lru.recent)
		}
	}
	lru.mu.Unlock()

	for _, v := range evicted {
		v.h.Release()
	}
}

func (lru *lru) Ban(n *Node) {
	lru.mu.Lock()
	if n.CacheData == nil {
		ln := lruNode{n: n, ban: true}
		n.CacheData = unsafe.Pointer(&ln)
	} else {
		ln := (*lruNode)(n.CacheData)
		if !ln.ban {
			ln.ban = true
			ln.remove()
			lru.used -= int64(n.size)
			lru.mu.Unlock()
			ln.h.Release()
			ln.h = nil
			return
		}
	}
	lru.mu.Unlock()
}

func (lru *lru) Evict(n *Node) {

	lru.mu.Lock()

	ln := (*lruNode)(n.CacheData)
	if ln == nil || ln.ban {
		return
	}

	n.CacheData = nil
	ln.remove()
	lru.used -= int64(n.size)
	lru.mu.Unlock()

	ln.h.Release()

}

func (lru *lru) EvictNS(ns int64) {
	lru.mu.Lock()
	var evicted []*lruNode
	for ln := lru.recent.next; ln != &lru.recent; {
		r := ln
		ln = lru.recent.next
		if r.n.ns == ns {
			node := r.n
			node.CacheData = nil
			ln.remove()
			lru.used -= int64(node.size)
			evicted = append(evicted, ln)
		}
	}
	lru.mu.Unlock()
	for _, ln := range evicted {
		ln.h.Release()
	}
}

func (lru *lru) EvictAll() {
	lru.mu.Lock()
	for ln := lru.recent.next; ln != &lru.recent; ln = ln.next {
		ln.n.CacheData = nil
	}
	lru.reset()
	lru.mu.Unlock()
	for ln := lru.recent.next; ln != &lru.recent; ln = ln.next {
		ln.h.Release()
	}
}

func (lru *lru) Close() error {
	return nil
}
