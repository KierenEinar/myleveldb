package utils

import (
	"sync"
)

// BytePool 对于bytes的池化
type BytePool struct {
	baseline [5]int64
	pools    [6]sync.Pool
}

// Put 将byte数组放入池子中
func (pool *BytePool) Put(b []byte) {
	idx := pool.poolNum(int64(cap(b)))
	b = b[:0] // reset 下
	pool.pools[idx].Put(&b)
}

// Get 获取len为size的byte数组
func (pool *BytePool) Get(n int64) []byte {
	idx := pool.poolNum(n)
	v := pool.pools[idx].Get().(*[]byte)
	if cap(*v) == 0 { // 需要初始化
		if idx == len(pool.baseline) { //说明落入了最后一个没有限制大小的区间中
			b := make([]byte, n)
			return b
		}
		return make([]byte, pool.baseline[idx])
	}

	// 取出来的不需要初始化了

	if int64(cap(*v)) >= n { // 容量大于等于请求的, 直接返回
		return (*v)[:n]
	}

	if idx == len(pool.baseline) { //说明落入了最后一个没有限制大小的区间中
		*v = make([]byte, n)
		return *v
	}

	*v = make([]byte, pool.baseline[idx])
	return *v
}

func (pool *BytePool) poolNum(capacity int64) int {

	idx := 0
	for ; idx < len(pool.baseline) && capacity > pool.baseline[idx]; idx++ {
	}
	return idx
}

func NewBytePool(baseline int64) *BytePool {
	return &BytePool{
		baseline: [...]int64{baseline >> 2, baseline >> 1, baseline, baseline << 1, baseline << 2},
		pools: [6]sync.Pool{
			{
				New: func() interface{} {
					return new([]byte)
				},
			},
			{
				New: func() interface{} {
					return new([]byte)
				},
			},
			{
				New: func() interface{} {
					return new([]byte)
				},
			},
			{
				New: func() interface{} {
					return new([]byte)
				},
			},
			{
				New: func() interface{} {
					return new([]byte)
				},
			},
			{
				New: func() interface{} {
					return new([]byte)
				},
			},
		},
	}
}
