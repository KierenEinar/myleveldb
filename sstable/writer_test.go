package sstable

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	datablockRI = 16 // restart interval
)

const (
	_1kb = 1 << ((iota + 1) * 10)
	_1mb
)

func Test_datablock(t *testing.T) {

	bw := newBlockWriter(datablockRI)

	chars := generateKeyValues()

	for _, v := range chars {
		bw.append([]byte(v), []byte(v))
	}

	bw.finish()

	assert.EqualValues(t, bw.entries, len(chars))
	t.Logf("entries=%d", bw.entries)

	br := newDataBlock(bw.buffer.Bytes(), nil, nil)
	bi := newBlockIter(br, nil)
	idx := 0
	for bi.Next() {
		//t.Logf("key=%s, v=%s", bi.Key(), bi.Value())
		assert.EqualValues(t, bi.Key(), chars[idx])
		assert.EqualValues(t, bi.Value(), chars[idx])
		idx++
	}

}

func generateKeyValues() []string {
	charS := []byte("a")
	size := len(charS)
	result := make([]string, 0)

	for {

		result = append(result, string(charS))

		c := charS[len(charS)-1]
		if c+1 == 'z'+1 {
			charS = append(charS, 'a')
		} else {
			charS[len(charS)-1]++
		}

		size += len(charS)
		if size >= _1mb {
			break
		}
	}

	return result

}
