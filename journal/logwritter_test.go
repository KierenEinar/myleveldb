package journal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter_Write(t *testing.T) {

	t.Run("测试写入4k的记录", func(t *testing.T) {

		writer := &Writer{
			writer: bytes.NewBuffer(nil),
		}

		chunk := generate4KChunk()
		n, err := writer.Write(chunk)
		assert.Nil(t, err)
		assert.EqualValues(t, n, 4096+headerSize)
	})

	t.Run("测试写入33k的记录", func(t *testing.T) {

		writer := &Writer{
			writer: bytes.NewBuffer(nil),
		}
		var chunk []byte
		for i := 0; i < 9; i++ {
			if i < 8 {
				chunk = generate4KChunk()
				_, err := writer.Write(chunk)
				assert.Nil(t, err)
				//assert.EqualValues(t, n, 4103)
			} else {
				chunk = generate1KChunk()
				_, err := writer.Write(chunk)
				assert.Nil(t, err)
				//assert.EqualValues(t, n, 1031)
			}
		}

	})

}

func generate4KChunk() []byte {
	return bytes.Repeat([]byte{0x1, 0x2, 0x3, 0x4}, 1024)
}

func generate1KChunk() []byte {
	return bytes.Repeat([]byte{0x1, 0x2, 0x3, 0x4}, 256)
}
