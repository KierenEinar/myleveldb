package myleveldb

import (
	"bytes"
	"encoding/binary"
)

/**

journal写入的chunk, 由batch组成

/-------------------------------------------/--------------------/--------------------------/--------------/
|	  			seq 8byte 				    | batch len 4 byte   |          batch 1         |    batch 2   |
/-------------------------------------------/--------------------/--------------------------/--------------/

note: batch len 实际上是 entry的数量

batch写入的entry格式

/----------/-----------------/---------------------/---------------/---------------------/
| 1byte	kt |  varint keylen	 |     	   key         |  varint vlen  |        value        |
/----------/-----------------/---------------------/---------------/---------------------/

**/

type Batch struct {
	data        *bytes.Buffer
	index       []BatchIndex
	internalLen int // ukey修改成内部的ikey后的总长度
	scratch     [binary.MaxVarintLen32]byte
}

// BatchIndex 索引
type BatchIndex struct {
	KeyType  keyType
	KeyPos   int
	KeyLen   int
	ValuePos int
	ValueLen int
}

func (b *Batch) BatchLen() uint32 {
	return uint32(len(b.index))
}

func (b *Batch) appendEntry(keyType keyType, key, value []byte) {

	n := 1 + binary.MaxVarintLen32 + len(key)
	if keyType == keyTypeVal {
		n += binary.MaxVarintLen32 + len(value)
	}

	batchIndex := BatchIndex{
		KeyType: keyType,
		KeyPos:  0,
		KeyLen:  len(key),
	}

	data := b.data.Bytes()

	// 扩容
	b.data.Grow(n)

	o := len(data)

	// 写入keytype, 并更新最后的下标
	b.data.WriteByte(byte(keyType))
	o++

	scratch := b.scratch[:]

	// 写入keylen, 并更新最后的下标
	m := binary.PutUvarint(scratch, uint64(len(key)))
	m, _ = b.data.Write(scratch[:m])
	o += m

	batchIndex.KeyPos = o
	batchIndex.KeyLen = len(key)
	// 写入key, 并更新最后的下标
	m, _ = b.data.Write(key)
	o += m

	if keyType == keyTypeVal {
		scratch = b.scratch[:]
		m = binary.PutVarint(scratch, int64(len(value)))
		o += m
		batchIndex.ValuePos = o
		batchIndex.ValueLen = len(value)
	}

	b.internalLen += len(key) + len(value) + 8 // 更新在memtable占用的bytes大小

	b.index = append(b.index, batchIndex)

}

//// DecodeBatch 解析batch
//func DecodeBatch(data []byte, fn func(kt keyType, key, value []byte)) {
//
//}

func (b *Batch) Put(key, value []byte) {
	b.appendEntry(keyTypeVal, key, value)
}

func (b *Batch) Delete(key []byte) {
	b.appendEntry(keyTypeDel, key, nil)
}

func (b *Batch) reset() {
	b.data.Reset()
	b.index = b.index[:0]
}
