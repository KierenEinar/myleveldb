package journal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

type flusher interface {
	flush() error
}

// Writer log record
type Writer struct {
	writer      io.Writer
	blockOffset int             // 当前的offset
	buf         [blockSize]byte //待写入的缓冲区
	f           flusher
}

// Write 写入一个chunk到journal中
func (w *Writer) Write(chunk []byte) (int, error) {

	// 待写入数据的offset下标
	chunkOffset := 0
	// 待写入数据的所有长度
	chunkLength := len(chunk)
	first := true
	end := false
	n := 0
	for {
		if chunkLength == 0 {
			break
		}
		// 获取当前block剩余的空间
		leftSpace := blockSize - w.blockOffset
		// 如果剩余的空间不够放一个header的长度, 全部补0
		if leftSpace < headerSize {
			if leftSpace > 0 {
				for i := 0; i < leftSpace; i++ {
					w.buf[w.blockOffset+i] = 0x0
				}
				n1, err := w.writeBlock(leftSpace)
				n += n1
				if err != nil {
					return n, err
				}
			}
			w.blockOffset = 0
			leftSpace = blockSize
		}

		// 可以被写入到block的长度
		available := leftSpace - headerSize

		// 当前要被写入的chunk的长度
		writeLen := chunkLength
		if available < chunkLength {
			writeLen = available
		}

		if available >= chunkLength {
			end = true
		}

		data := chunk[chunkOffset : chunkOffset+writeLen]

		// 设置record header check sum
		binary.LittleEndian.PutUint32(w.buf[w.blockOffset:], crc32.ChecksumIEEE(data))
		binary.LittleEndian.PutUint16(w.buf[w.blockOffset+4:], uint16(writeLen))

		if first && end {
			w.buf[w.blockOffset+6] = recordTypeFull
		} else if first && !end {
			w.buf[w.blockOffset+6] = recordTypeFirst
		} else if !first && end {
			w.buf[w.blockOffset+6] = recordTypeLast
		} else {
			w.buf[w.blockOffset+6] = recordTypeMiddle
		}

		copy(w.buf[w.blockOffset+headerSize:], data)
		n2, err := w.writeBlock(writeLen + headerSize)
		n += n2
		if err != nil {
			return n, err
		}
		w.blockOffset += n2
		chunkOffset += (n2 - headerSize)
		first = false
		chunkLength -= (n2 - headerSize)
	}

	return n, nil

}

func (w *Writer) writeBlock(length int) (int, error) {

	n, err := w.writer.Write(w.buf[w.blockOffset : w.blockOffset+length])
	if err != nil {
		return n, err
	}
	if w.f != nil {
		err = w.f.flush()
	}

	return n, err
}
