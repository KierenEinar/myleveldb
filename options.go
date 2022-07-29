package myleveldb

import (
	"myleveldb/comparer"
	"myleveldb/utils"
)

const (
	writerEnable                      = true
	defaultMemDbWriterBuffer          = 1 << 22 // 4m
	defaultSStableDataBlockSize int64 = 1 << 11 // 2k
)

// Options db相关的选项
type Options struct {
	ReadOnly bool // 是否只读模式打开

	WriteBuffer int // memdb的容量

	Cmp comparer.BasicComparer // 比较大小

	SSTableDataBlockSize int64 // sstable的datablock的大小

}

func (opt *Options) GetPool() *utils.BytePool {
	dataBlockSize := defaultSStableDataBlockSize
	if opt != nil {
		dataBlockSize = opt.SSTableDataBlockSize
	}
	return utils.NewBytePool(dataBlockSize) //todo
}

func (opt *Options) GetReadOnly() bool {
	if opt == nil {
		return writerEnable
	}
	return opt.ReadOnly
}

func (opt *Options) GetWriteBuffer() int {
	if opt == nil || opt.WriteBuffer == 0 {
		return defaultMemDbWriterBuffer
	}
	return opt.WriteBuffer
}

func (opt *Options) GetCompare() comparer.BasicComparer {
	if opt == nil || opt.Cmp == nil {
		return comparer.DefaultComparer
	}

	return opt.Cmp
}
