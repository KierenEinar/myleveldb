package journal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

/**

使用方式
r := &Reader{
	reader: buf,
}

for {

	reader, err := r.SeekNextChunk()
	if err == io.EOF {
		break
	}
	if err != nil {
		return nil, err
	}

	if _, err := ioutil.ReadAll(reader); err != nil {
		if err == io.ErrUnExpectedEOF {
			continue
		}
		return nil, err
	}; else {
		该干嘛干嘛
	}

}


**/

// Reader log record
type Reader struct {
	reader io.Reader
	j      int             // 上一把内容有效的最后位置
	i      int             // 上一把内容有效的开始位置
	last   bool            // 当前的chunk是否是record的最后一个
	n      int             // 当前已经读到reader的第n个下标
	buf    [blockSize]byte // 目前读入到的一整块block
}

func NewReader(reader io.Reader) *Reader {
	return &Reader{
		reader: reader,
	}
}

type chunkReader struct {
	r *Reader
}

// SeekNextChunk 寻找下一个chunk的起始
func (r *Reader) SeekNextChunk() (io.Reader, error) {

	r.i = r.j

	for {
		if err := r.readRecord(true); err == nil {
			return &chunkReader{r}, nil
		} else if err == ErrSkipped {
			continue
		} else {
			return nil, err
		}
	}
}

// 读取一个record的i, j
func (r *Reader) readRecord(firstPartOfChunk bool) error {
	for {
		// 说明剩下的内容够读一个record
		if r.j+headerSize <= r.n {

			checkSum := binary.LittleEndian.Uint32(r.buf[r.j : r.j+4])
			length := binary.LittleEndian.Uint16(r.buf[r.j+4 : r.j+6])
			recordType := r.buf[r.j+6]

			if checkSum == 0 && length == 0 && recordType == 0 {
				r.i, r.j = r.n, r.n // 丢掉整个block
				return ErrSkipped
			}
			calculateCheckSum := crc32.ChecksumIEEE(r.buf[r.j+headerSize : r.j+headerSize+int(length)])
			if calculateCheckSum != checkSum {
				r.i, r.j = r.n, r.n // 丢掉整个block
				return ErrSkipped
			}

			r.i = r.j + headerSize
			r.j = r.j + headerSize + int(length)

			if r.j > r.n {
				r.i, r.j = r.n, r.n
				return ErrSkipped
			}

			if firstPartOfChunk && recordType != recordTypeFirst && recordType != recordTypeFull {
				r.i = r.j // 丢掉这个record
				return ErrSkipped
			}

			r.last = recordType == recordTypeFull || recordType == recordTypeLast

			return nil

		}
		// 如果当前剩余的容量不够读一个record并且是最后一块block
		if r.n < blockSize && r.n > 0 {
			return io.EOF
		}

		// 剩下的情况为需要读取新的一整块block
		n, err := io.ReadFull(r.reader, r.buf[:])
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return err
		}
		if n == 0 {
			return io.EOF // 没有可读的了
		}
		r.i, r.j, r.n = 0, 0, n
	}
}

func (r *chunkReader) ReadByte() (byte, error) {

	reader := r.r

	if reader.i != reader.j {
		c := reader.buf[reader.i]
		reader.i++
		return c, nil
	}

	for {

		if reader.last {
			return 0, io.EOF
		}

		err := reader.readRecord(false)

		if err == ErrSkipped {
			return 0, io.ErrUnexpectedEOF
		}

		if err != nil {
			return 0, err
		}

		c := reader.buf[reader.i]
		reader.i++
		return c, nil
	}

}

// 使用ioutil.ReadAll(r), 便可把整个chunk的内容吐出
func (r *chunkReader) Read(p []byte) (n int, err error) {
	reader := r.r
	// 直接读取[i, j]的数据
	if reader.i != reader.j {
		n = copy(p, reader.buf[reader.i:reader.j])
		reader.i += n
		return
	}

	for {

		if reader.last {
			return 0, io.EOF
		}

		err := reader.readRecord(false)

		if err == ErrSkipped {
			return 0, io.ErrUnexpectedEOF
		}

		if err != nil {
			return 0, err
		}

		n := copy(p, reader.buf[reader.i:reader.j])

		reader.i += n

		return n, nil

	}

}
