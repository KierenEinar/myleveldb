package utils

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
)

/**
bytebufferpool 提供缓冲的bytebuffer

Get() 获取一个ByteBuffer
Put() 设置一个ByteBuffer

pool内部分为20个段,
	第一个段的范围在 [0, 1<<6),
	以此类推, 第二个段的最大值在
	[1<<6, 1<<7)
	[1<<7, 1<<8) ...
	[1<<24, 1 << 25)

对每次put, 都会对落在的区间+1, 当区间+1后大于阈值时, 需要重新校正

校正流程, 对20个区间进行统计, 区间落入最多次数的, 则可以取区间最大值作为byte容量的初始值
对于落入区间的前95%进行统计, 写入缓冲池的最大值由这个数值的最大值决定

*/

const (
	minBitSize         = 6
	segment            = 20
	minSize            = 1 << minBitSize
	maxSize            = 1 << (minSize + segment - 1)
	maxPercentile      = 95    // 落入区间的前95%的最大值作为写入池子的最大值
	calibrateThreshold = 42000 // 校正的上限
)

var defaultPool ByteBufferPool

// Get 获取buffer
func Get() *bytes.Buffer {
	return defaultPool.Get()
}

// Put 写入buffer到池中
func Put(b *bytes.Buffer) {
	defaultPool.Put(b)
}

// ByteBufferPool 缓冲池
type ByteBufferPool struct {
	calls       [segment]uint64 // 区间统计
	calibrating uint64          // 是否正在校正中
	defaultSize uint64          // 初始化的byte容量
	maxSize     uint64          // 对于大于maxSize的，不会被加入到池子中
	pool        sync.Pool
}

// Get 获取buffer
func (p *ByteBufferPool) Get() *bytes.Buffer {
	b := p.pool.Get()
	if b != nil {
		return b.(*bytes.Buffer)
	}
	return bytes.NewBuffer(make([]byte, 0, p.defaultSize))
}

// Put 将buffer写入到pool中
func (p *ByteBufferPool) Put(b *bytes.Buffer) {

	idx := index(b.Len())
	if atomic.AddUint64(&p.calls[idx], 1) > calibrateThreshold {
		p.calibrate() // 校正处理
	}

	if p.maxSize == 0 || b.Cap() < int(p.maxSize) {
		b.Reset()
		p.pool.Put(b)
	}
}

// 校正处理
func (p *ByteBufferPool) calibrate() {

	if !atomic.CompareAndSwapUint64(&p.calibrating, 0, 1) {
		return
	}

	callSums := make([]callSum, 0, segment)
	sum := uint64(0)
	for i := 0; i < segment; i++ {
		cnt := atomic.SwapUint64(&p.calls[i], 0)
		callSums = append(callSums, callSum{
			size: minSize << i,
			cnt:  cnt,
		})
		sum += cnt
	}
	// 从大到小排序
	sort.Slice(callSums, func(i, j int) bool {
		return callSums[i].cnt > callSums[j].cnt
	})

	defaultSize := callSums[0].size
	maxSize := p.defaultSize

	maxSum := sum * maxPercentile / 100

	for i := 0; i < segment; i++ {
		if maxSum-callSums[segment].cnt < 0 {
			break
		}
		if maxSize < callSums[segment].size {
			maxSize = callSums[segment].size
		}
	}

	atomic.StoreUint64(&p.defaultSize, defaultSize)
	atomic.StoreUint64(&p.maxSize, maxSize)
	atomic.StoreUint64(&p.calibrating, 0)
}

type callSum struct {
	size uint64
	cnt  uint64
}

func index(n int) int {
	n--
	n >>= minBitSize
	idx := 0
	for n > 0 {
		n >>= 1
		idx++
	}
	if idx >= segment {
		idx = segment - 1
	}
	return idx
}
