package journal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	blockSize  = 32 << 10 // 32k
	headerSize = 7        // journal的头部占用7个字节 4byte的checksum| 2byte的 data占用字节 | 1byte的类型 | []data
)

const (
	kFullType = iota
	kFirstType
	kMiddleType
	kLastType
)

type flusher interface {
	Flush() error
}

// Writer 操控chunk写入
type Writer struct {
	w       io.Writer       // 持久化的io writer
	i, j    int             // 上一次写入后chunk的首尾下标
	pending bool            // 是否当前buf中已经有写入了, 但是还没有真正通过io.writer写出去
	written int             // 目前buf中有多少是已经写入到io.writer了
	buf     [blockSize]byte // 缓冲区
	first   bool            // 当前要写入的chunk是否为本次要写入所有chunk的第一个
	f       flusher         // 写完调用的flush方法
	err     error           // 保存当前上下文的错误
}

type singleWriter struct {
	w *Writer
}

func (w *Writer) writePending() error {
	if w.pending {
		w.fillHeader(true)
		w.pending = false
	}
	_, err := w.w.Write(w.buf[w.written:w.j])
	w.written = w.j
	return err
}

// 将当前buf[written:]写入到 io.writer中
func (w *Writer) writeBlock() error {
	if _, err := w.w.Write(w.buf[w.written:]); err != nil {
		return err
	}
	w.i = 0
	w.j = headerSize
	w.written = 0
	return nil
}

// 填充上一个chunk的header
func (w *Writer) fillHeader(last bool) {
	if last {
		if w.first {
			w.buf[w.i+6] = kFullType
		} else {
			w.buf[w.i+6] = kLastType
		}
	} else {
		if w.first {
			w.buf[w.i+6] = kFirstType
		} else {
			w.buf[w.i+6] = kMiddleType
		}
	}
	binary.LittleEndian.PutUint32(w.buf[w.i:w.i+4], crc32.ChecksumIEEE(w.buf[w.i+6:w.j]))
	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-headerSize))
}

// Next
func (w *Writer) Next() (io.Writer, error) {

	if w.pending {
		w.fillHeader(true) // 填充上一个chunk的头部
	}

	w.i = w.j
	w.j = w.j + headerSize

	// 如果当前block剩下的可填充字节小于一个header的字节大小, 那么把当前block都填完
	if w.j > blockSize {
		for k := w.i; k < blockSize; k++ {
			w.buf[k] = 0
		}
		w.err = w.writeBlock() // 填充整个block
		if w.err != nil {
			return nil, w.err
		}
	}

	w.first = true
	w.pending = true
	return &singleWriter{w: w}, nil
}

// 遵循io.writer接口
func (x *singleWriter) Write(p []byte) (int, error) {
	w := x.w
	n0 := len(p)
	for len(p) > 0 {
		if w.j == blockSize {
			w.fillHeader(false)
			w.err = w.writeBlock()
			if w.err != nil {
				return 0, w.err
			}
			w.first = false
		}

		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return n0, nil
}

// Flush 刷新chunk到io.writer中
func (w *Writer) Flush() error {

	w.err = w.writePending()
	if w.err != nil {
		return w.err
	}

	if w.f != nil {
		if w.err = w.f.Flush(); w.err != nil {
			return w.err
		}
	}

	return nil
}
