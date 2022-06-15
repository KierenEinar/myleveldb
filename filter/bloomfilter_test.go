package filter

import (
	"bytes"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

const (
	_1kb = 1 << ((iota + 1) * 10)
	_1mb
)

func TestBloomFilterGenerator(t *testing.T) {

	bloomFilter := &BloomFilter{}
	generator := bloomFilter.NewFilterGenerator(defaultBitsPerKey)
	keys := rand16MbKeys()

	for idx := 0; idx < len(keys)/4; idx++ {
		generator.Add(keys[idx*4 : (idx+1)*4])
	}
	buf := bytes.NewBuffer(nil)
	generator.Generate(buf)

	data := buf.Bytes()

	for idx := 0; idx < len(keys)/4; idx++ {
		ok := bloomFilter.Contains(data, keys[idx*4:(idx+1)*4])
		assert.True(t, ok)
	}

	assert.False(t, bloomFilter.Contains(data, []byte("foo")))
	assert.False(t, bloomFilter.Contains(data, []byte("bar")))
}

// 随机生成总大小为16mb的key数组
func rand16MbKeys() []byte {

	perKeyBytes := 4 // 4个字节, 用int32代替
	_16mb := 1 << 4 * _1mb
	result := make([]byte, 0, _16mb)
	total := _16mb / perKeyBytes
	b := make([]byte, 4)
	for idx := 0; idx < total; idx++ {
		randKey := rand.Uint32()
		binary.LittleEndian.PutUint32(b, randKey)
		result = append(result, b...)
		//b = b[:0] // reset
	}
	return result
}
