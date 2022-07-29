package myleveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	error2 "myleveldb/error"
	"myleveldb/memdb"
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

const (
	batchHeaderLen = 12
)

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

func (bi *BatchIndex) key(data []byte) []byte {
	return data[bi.KeyPos:bi.KeyLen]
}

func (bi *BatchIndex) value(data []byte) []byte {
	return data[bi.ValuePos:bi.ValueLen]
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

func (b *Batch) Put(key, value []byte) {
	b.appendEntry(keyTypeVal, key, value)
}

func (b *Batch) Delete(key []byte) {
	b.appendEntry(keyTypeDel, key, nil)
}

func (b *Batch) reset() {
	b.data.Reset()
	b.index = b.index[:0]
	b.internalLen = 0
}

func writeBatchWithHeader(writer io.Writer, seq uint64, b *Batch) error {

	header := make([]byte, batchHeaderLen)

	binary.LittleEndian.PutUint64(header, seq)
	binary.LittleEndian.PutUint32(header[8:], b.BatchLen())

	_, err := writer.Write(header)
	if err != nil {
		return err
	}

	_, err = writer.Write(b.data.Bytes())
	if err != nil {
		return err
	}
	return nil

}

func decodeBatchToMem(chunk []byte, expectSeq uint64, memDb *memdb.MemDB) (seq uint64, batchLen int, err error) {

	header := chunk[:batchHeaderLen]

	seq = binary.LittleEndian.Uint64(header[:8])
	batchLen = int(binary.LittleEndian.Uint32(header[8:]))

	if seq < expectSeq {
		err = error2.NewBatchDecodeHeaderErrWithSeq(expectSeq, seq)
		return
	}

	data := chunk[batchHeaderLen:]

	err = decodeBatch(data, batchLen, func(idx int, key []byte, kt keyType, value []byte) error {
		return memDb.Put(makeInternalKey(key, seq+uint64(idx), kt), value)
	})

	return

}

func decodeBatch(data []byte, batchLen int, f func(idx int, key []byte, kt keyType, value []byte) error) error {

	pos := 0
	end := len(data)
	idx := 0
	for ; pos < end; idx++ {

		// decode keytype
		if pos+1 > end {
			return fmt.Errorf("decode key type pos out of range")
		}
		kt := keyType(data[pos : pos+1][0])
		pos += 1

		// decode klen
		kLen, n := binary.Varint(data[pos:])
		pos += n

		// decode key
		if pos+int(kLen) >= end {
			return fmt.Errorf("decode key pos out of range")
		}
		key := data[pos : pos+int(kLen)]

		if kt == keyTypeDel {
			err := f(idx, key, kt, nil)
			if err != nil {
				return err
			}
			continue
		}

		vLen, m := binary.Varint(data[pos:])
		pos += m
		if pos+int(vLen) >= end {
			return fmt.Errorf("decode value pos out of range")
		}

		v := data[pos : pos+int(vLen)]

		err := f(idx, key, kt, v)
		if err != nil {
			return err
		}

	}

	if idx != batchLen-1 {
		return fmt.Errorf("batch decode, expectEntryLen=%d, realEntryLen=%d", batchLen, idx+1)
	}

	return nil

}
