package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"myleveldb/filter"
	"myleveldb/utils"
)

type compressionType uint

const (
	noCompress compressionType = 0 // 不压缩
	compress   compressionType = 1 // 压缩
)

// blockWriter 构建一个block
type blockWriter struct {
	buffer          *bytes.Buffer
	prevKey         []byte
	entries         int
	restartInterval int
	restartIndexes  []int
	scratch         [30]byte
}

func newBlockWriter(restartInterval int) *blockWriter {
	return &blockWriter{
		buffer:          utils.Get(),
		restartInterval: restartInterval,
	}
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
		bw.restartIndexes = append(bw.restartIndexes, bw.buffer.Len())
	} else {
		sharedKeyLen = len(getSharedPart(key, bw.prevKey))
	}

	unSharedKeyLen = len(key) - sharedKeyLen

	n0 := binary.PutUvarint(bw.scratch[0:], uint64(sharedKeyLen))
	n1 := binary.PutUvarint(bw.scratch[n0:], uint64(unSharedKeyLen))
	n2 := binary.PutUvarint(bw.scratch[n0+n1:], uint64(len(value)))

	bw.buffer.Write(bw.scratch[:n0+n1+n2])
	bw.buffer.Write(key[sharedKeyLen:])
	bw.buffer.Write(value)
	bw.prevKey = append(bw.prevKey[:0], key...)
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
	bw.buffer.Grow(len(bw.restartIndexes) * 4)
	for _, v := range bw.restartIndexes {
		binary.LittleEndian.PutUint32(scratch, uint32(v))
		bw.buffer.Write(scratch)
	}
}

func (bw *blockWriter) reset() {
	bw.buffer.Reset()
	bw.entries = 0
	bw.restartIndexes = bw.restartIndexes[:0]
}

// BlockSize 获取当前block块的大小
func (bw *blockWriter) BlockSize() int {
	return bw.buffer.Len() + len(bw.restartIndexes)*4 + 4
}

// 过滤器写入相关
type filterWriter struct {
	writer          io.Writer
	buf             *bytes.Buffer
	baseLg          uint32
	filterGenerator filter.IFilterGenerator
	offsets         []uint32
	keys            int
}

// 加入过滤器中
func (fb *filterWriter) add(key []byte) {
	if fb.filterGenerator == nil {
		return
	}
	fb.filterGenerator.Add(key)
	fb.keys++
}

// 生成一段bit位图后丢入本身的buffer中
func (fb *filterWriter) generate(offset uint64) {

	if fb.filterGenerator == nil {
		return
	}

	for offset/(1<<fb.baseLg) > uint64(len(fb.offsets)) {
		fb.offsets = append(fb.offsets, uint32(fb.buf.Len()))
		if fb.keys > 0 {
			fb.filterGenerator.Generate(fb.buf)
			fb.keys = 0
		}
	}
}

// 完成一个block的写入
func (fb *filterWriter) finish() {

	// 先把当前最后的offset写入(如果当前已经没有key值了, 那么offset为offset's offset, 反之为上一个block的offset)
	fb.offsets = append(fb.offsets, uint32(fb.buf.Len()))

	if fb.keys > 0 { // 如果当前集合存在未写入的key值
		fb.filterGenerator.Generate(fb.buf)                   // 那么生成过滤器bits
		fb.offsets = append(fb.offsets, uint32(fb.buf.Len())) // 写入offset's offset
		fb.keys = 0
	}

	fb.buf.Grow(len(fb.offsets)*4 + 1)

	tmp := make([]byte, 4)
	for _, offset := range fb.offsets {
		binary.LittleEndian.PutUint32(tmp, offset)
		fb.buf.Write(tmp)
		tmp = tmp[:0]
	}
	fb.buf.WriteByte(byte(fb.baseLg))

}

// Writer 将内容写入sstable的writer
type Writer struct {
	io.Writer
	filter                                     *filter.BloomFilter
	err                                        error
	blockSize                                  int             // data block的块大小
	compressionType                            compressionType // 压缩类型
	pendingBlockHandle                         *blockHandle
	offset                                     uint64
	dataBlockWriter                            *blockWriter
	filterBlockWriter                          *filterWriter
	metaIndexBlockWriter, dataIndexBlockWriter *blockWriter
	scratch                                    [50]byte
}

// Append 将内容写入到sstable
func (w *Writer) Append(key, value []byte) {

	// 如果上一次添加后block的大小超过block size, 那么存在block handle, 需要写入index block中
	w.flushPendingBH(key)

	w.dataBlockWriter.append(key, value)

	w.filterBlockWriter.add(key)

	// 如果当前block写满了, 那么直接写入writer中
	if w.dataBlockWriter.BlockSize() >= w.blockSize {
		w.err = w.finishBlock(w.dataBlockWriter)
	}

}

// Close 关闭实现
// 暂且默认布隆过滤器是打开的, 因为显著提高性能
func (w *Writer) Close() error {

	var err error

	// 如果data block还有没写入到设备块的, 那么写入到设备块
	if w.dataBlockWriter.buffer.Len() > 0 {
		// 先把block handle 刷到 index block中
		w.flushPendingBH(nil)
		// 写入一个block
		err = w.finishBlock(w.dataBlockWriter)
		if err != nil {
			return err
		}
	}

	var filterBh *blockHandle

	// 将bloom filter的内容写入到设备块中
	w.filterBlockWriter.finish()
	filterBh, err = w.writeBlock(w.filterBlockWriter.buf, noCompress)
	if err != nil {
		return err
	}

	var metaBlockHandle *blockHandle
	// 写入metablock handle
	key := []byte("filter." + w.filter.Name())
	n := encodeBlockHandle(w.scratch[:20], *filterBh)
	w.metaIndexBlockWriter.append(key, w.scratch[:n])
	metaBlockHandle, err = w.writeBlock(w.metaIndexBlockWriter.buffer, w.compressionType)
	if err != nil {
		return err
	}

	// 写入indexblock
	var indexBlockHandle *blockHandle
	w.dataIndexBlockWriter.finish()
	indexBlockHandle, err = w.writeBlock(w.dataIndexBlockWriter.buffer, w.compressionType)
	if err != nil {
		return err
	}

	footer := make([]byte, footerLength)

	n = encodeBlockHandle(footer, *metaBlockHandle)
	_ = encodeBlockHandle(footer[n:], *indexBlockHandle)

	copy(footer[footerLength-len(magic):], magic)

	_, err = w.Writer.Write(footer)
	if err != nil {
		return err
	}
	return nil

}

// 将block缓冲中的数据写入到块设备中
func (w *Writer) finishBlock(block *blockWriter) error {

	// 将当前restartIndexes写入到buffer中
	block.finish()

	// 真正的将block写入到设备块中
	bh, err := w.writeBlock(block.buffer, w.compressionType)

	if err != nil {
		return err
	}

	w.pendingBlockHandle = bh

	w.dataBlockWriter.reset()

	w.filterBlockWriter.generate(w.offset)

	return nil
}

func (w *Writer) writeBlock(buffer *bytes.Buffer, compressionType compressionType) (*blockHandle, error) {

	var buf []byte

	if compressionType == compress {

	} else {
		buffer.Grow(5)
		buffer.WriteByte(blockTypeNoCompression)
		buf = buffer.Bytes()
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
