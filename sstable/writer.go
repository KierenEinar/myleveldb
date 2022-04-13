package sstable

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

type compressionType uint

const (
	noCompress compressionType = 0 // 不压缩
	compress   compressionType = 1 // 压缩
)

// blockWriter 构建一个block
type blockWriter struct {
	buffer          []byte
	prevKey         []byte
	entries         int
	restartInterval int
	restartIndexes  []int
	scratch         [30]byte
}

// growBuffer 扩张blockwriter的buffer
func (bw *blockWriter) growBuffer(n int) int {

	c := cap(bw.buffer)
	m := len(bw.buffer)

	var dst []byte

	// 说明当前cap还够再放, 不需要
	if c+n <= m {
		dst = bw.buffer[:m+n]
		goto end
	}

	if bw.buffer == nil && n <= 1<<6 {
		dst = make([]byte, n, 1<<6)
		goto end
	}

	// 说明当前n+m > c, 扩容 1倍
	dst = make([]byte, powerOfTwo(c+n))
	copy(dst, bw.buffer)
end:
	bw.buffer = dst
	return m
}

func getSharedPart(a, b []byte) []byte {
	j := len(a)
	if len(b) < j {
		j = len(b)
	}
	for i := 0; i < j; i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return nil
}

func (bw *blockWriter) append(key, value []byte) {

	var (
		sharedKeyLen   int
		unSharedKeyLen int
	)

	if bw.entries%bw.restartInterval == 0 {
		bw.restartIndexes = append(bw.restartIndexes, len(bw.buffer))
	} else {
		sharedKeyLen = len(getSharedPart(key, bw.prevKey))
		unSharedKeyLen = len(key) - sharedKeyLen
	}

	n := binary.PutUvarint(bw.scratch[:], uint64(sharedKeyLen))
	m := binary.PutUvarint(bw.scratch[n:], uint64(unSharedKeyLen))
	_ = binary.PutUvarint(bw.scratch[n+m:], uint64(len(value)))
	bw.buffer = append(bw.buffer, bw.scratch[:]...)
	bw.buffer = append(bw.buffer, key[unSharedKeyLen:]...)
	bw.buffer = append(bw.buffer, value...)

	bw.entries++

}

// 结束一个block, 主要是把restartIndex填入buffer中
func (bw *blockWriter) finish() {
	if bw.entries == 0 {
		bw.restartIndexes = append(bw.restartIndexes, 0)
	}
	length := len(bw.restartIndexes)
	bw.restartIndexes = append(bw.restartIndexes, length)
	scratch := make([]byte, 4)
	for _, v := range bw.restartIndexes {
		scratch = scratch[:0]
		binary.LittleEndian.PutUint32(scratch, uint32(v))
		bw.buffer = append(bw.buffer, scratch...)
	}
}

func (bw *blockWriter) reset() {
	bw.buffer = bw.buffer[:0]
	bw.entries = 0
	bw.restartIndexes = bw.restartIndexes[:0]
}

// BlockSize 获取当前block块的大小
func (bw *blockWriter) BlockSize() int {
	return len(bw.buffer) + len(bw.restartIndexes)*4 + 4
}

// Writer 将内容写入sstable的writer
type Writer struct {
	io.Writer
	err                  error
	blockSize            int             // data block的块大小
	compressionType      compressionType // 压缩类型
	pendingBlockHandle   *blockHandle
	offset               uint64
	dataBlockWriter      *blockWriter
	dataIndexBlockWriter *blockWriter
	scratch              [50]byte
}

// Append 将内容写入到sstable
func (w *Writer) Append(key, value []byte) {

	// 如果上一次添加后block的大小超过block size, 那么存在block handle, 需要写入index block中
	w.flushPendingBH(key)

	w.dataBlockWriter.append(key, value)

	w.filterBlock.append(key)

	// 如果当前block写满了, 那么直接写入writer中
	if w.dataBlockWriter.BlockSize() >= w.blockSize {
		w.err = w.finishBlock(w.dataBlockWriter)
	}

}

// 将block缓冲中的数据写入到块设备中
func (w *Writer) finishBlock(block *blockWriter) error {

	// 将当前restartIndexes写入到buffer中
	block.finish()

	// 真正的将block写入到设备块中
	bh, err := w.writerBlock(block, w.compressionType)

	if err != nil {
		return err
	}

	w.pendingBlockHandle = bh

	w.dataBlockWriter.reset()

	w.filterBlock.finishBlock(w.offset)

	return nil
}

func (w *Writer) writerBlock(block *blockWriter, compressionType compressionType) (*blockHandle, error) {

	var buf []byte

	if compressionType == compress {

	} else {
		n := block.growBuffer(5)
		block.buffer[n] = blockTypeNoCompression
		buf = block.buffer
	}

	checkSum := crc32.ChecksumIEEE(buf)
	binary.LittleEndian.PutUint32(buf[len(buf):], checkSum)
	_, err := w.Writer.Write(buf)
	if err != nil {
		return nil, err
	}

	bh := &blockHandle{
		offset: w.offset,
		length: uint64(len(buf) - blockTrialLen),
	}

	w.offset += uint64(len(buf))
	return bh, nil
}

func (w *Writer) flushPendingBH(key []byte) {
	if w.pendingBlockHandle == nil {
		return
	}

	indexKey := w.getIndexKey(w.dataBlockWriter.prevKey, key)
	dst := make([]byte, 10)
	n := encodeBlockHandle(dst, *w.pendingBlockHandle)
	w.dataIndexBlockWriter.append(indexKey, dst[:n])
	w.pendingBlockHandle = nil
	w.dataBlockWriter.prevKey = w.dataBlockWriter.prevKey[:0]
}

// 获取index block的key
func (w *Writer) getIndexKey(a, b []byte) []byte {
	var separator []byte
	if b == nil {
		separator = getSuccessor(a)
	} else {
		separator = getSeparator(a, b)
	}
	return separator
}

/**
*	遍历字节数组a, 从左往右遍历, 找到第一个ascii+1并且+1后的字节表示不超过255 即可返回
*	特殊情况下, 如果a从左往右每一位都是 0xff, 则直接返回 a
*	e.g.
*	0xff 0xff 0xfe -> 0xff 0xff 0xff
*	0xfe -> 0xff
*   0xf1 0xff -> 0xf2
**/
func getSuccessor(a []byte) []byte {

	for i, c := range a {
		if c+1 < 0xff {
			dst := make([]byte, i+1)
			copy(dst, a[:i])
			dst[i] = c + 1
			return dst
		}
	}
	return a
}

/**
*   要求是a<=b
*	获取a, b 之间的separator, 返回的x是 a<=x, x<b
*	存在一种特殊情况, 如果a是b的子集, 那么直接返回a本身
*	从左往右, 遍历a, b, 相同的跳过, 直到找到第一个a[i] < b[i] 并且 a[i]+1 < 一个字节的最大值(0xff)
*   以此类推, 找不到的时候回返回a 本身
*	e.g.
*	ae   b     -> ae
*	abcd abcde -> abcd
*	abcd abcf  -> abce
*	abcd abcd  -> abcd
*	0xff 0xfe | 0xff 0xff -> 0xff 0xfe
*
**/
func getSeparator(a, b []byte) []byte {

	// 找出两个之间最短的
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for ; a[i] == b[i] && i < n; i++ {

	}

	if i >= n {
		// 说明a是b的子集
	} else if c := a[i] + 1; c < b[i] && c < 0xff {
		dst := append(a[:i], c)
		return dst
	}

	return a
}

func powerOfTwo(givenMum int) int {
	givenMum--
	givenMum |= givenMum >> 1
	givenMum |= givenMum >> 2
	givenMum |= givenMum >> 4
	givenMum |= givenMum >> 8
	givenMum |= givenMum >> 16
	givenMum |= givenMum >> 32
	givenMum++
	return givenMum
}
