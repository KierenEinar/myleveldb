package myleveldb

import (
	"encoding/binary"
	"errors"
)

// key的类型
// 有增加, 删除
type keyType uint8

const (
	keyTypeVal = keyType(1) // 增加
	keyTypeDel = keyType(2) // 删除
)

const (
	maxSeq = uint64(1<<56) - 1
)

// ukey + seq << 8 | keyType
type internalKey []byte

func makeInternalKey(key []byte, seq uint64, kt keyType) internalKey {
	if seq > maxSeq {
		panic("seq invalid")
	}

	if kt > keyTypeDel {
		panic("key type invalid")
	}

	dst := ensureBuffer(len(key) + 8)
	copy(dst, key)

	binary.LittleEndian.PutUint64(dst[len(key):], seq<<8|uint64(kt))
	return dst
}

func parseInternalKey(ik []byte) (uk []byte, seq uint64, kt keyType, err error) {
	if len(ik) < 8 {
		err = errors.New("invalid ik, len less than 8")
		return
	}
	uk = ik[:len(ik)-8]
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	seq = x >> 8
	kt = keyType(x & (0xff))
	if kt > keyTypeDel {
		return nil, 0, 0, errors.New("invalid key type")
	}
	return
}

func (ik internalKey) uKey() []byte {
	ik.assert()
	return ik[:len(ik)-8]
}

func (ik internalKey) num() uint64 {
	ik.assert()
	return binary.LittleEndian.Uint64(ik[len(ik)-8:])
}

func (ik internalKey) assert() {
	if ik == nil {
		panic("ik is nil")
	}
	if len(ik) < 8 {
		panic("invalid ik, len less than 8")
	}
}

func ensureBuffer(n int) []byte {
	return make([]byte, n)
}
