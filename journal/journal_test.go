package journal

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkReader_Read(t *testing.T) {

	t.Run("测试读取4k的记录", func(t *testing.T) {

		writer := &Writer{
			writer: bytes.NewBuffer(nil),
		}

		chunk := generate4KChunk()
		// 分割成每32byte 一个chunk

		for idx := 0; idx < 4096/32; idx++ {
			i := idx * 4
			j := i + 32
			data := chunk[i:j]
			n, err := writer.Write(data)
			assert.Nil(t, err)
			assert.EqualValues(t, n, 32)
		}

		reader := &Reader{
			reader: bytes.NewReader(writer.buf[:len(chunk)]),
		}

		for {

			r, err := reader.SeekNextChunk()
			if err == io.EOF {
				return
			}
			if err != nil {
				t.Fatal(err)
			}

			data, err := ioutil.ReadAll(r)
			assert.Nil(t, err)
			t.Logf("%v", data)
		}

	})

	t.Run("测试写入33k的记录", func(t *testing.T) {

		ioWriter := bytes.NewBuffer(nil)

		writer := &Writer{
			writer: ioWriter,
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

		n, err := writer.Write([]byte("hello"))
		assert.Nil(t, err)
		assert.EqualValues(t, n, 12)
		n, err = writer.Write([]byte("apple juice"))
		assert.Nil(t, err)
		assert.EqualValues(t, n, 18)
		n, err = writer.Write([]byte("apple rice"))
		assert.Nil(t, err)
		assert.EqualValues(t, n, 17)

		reader := &Reader{
			reader: bytes.NewReader(ioWriter.Bytes()),
		}

		for {

			r, err := reader.SeekNextChunk()
			if err == io.EOF {
				return
			}
			if err != nil {
				t.Fatal(err)
			}

			data, err := ioutil.ReadAll(r)
			assert.Nil(t, err)
			fmt.Printf("%v\n", data)
		}

	})

}
