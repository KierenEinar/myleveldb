package myleveldb

import (
	"math"
	"myleveldb/comparer"
	"myleveldb/utils"
)

const (
	mb = 1 << 20

	writerEnable                      = true
	defaultMemDbWriterBuffer          = 1 << 22 // 4m
	defaultSStableDataBlockSize int64 = 1 << 11 // 2k
	defaultSStableFileSize            = 1 << 11 // 2m

	// 默认当要扩大输入文件时, 一次性总共不能超过25个文件进行合并
	defaultCompactionLimitFiles = 25

	// 写入的时候进行检查, 如果level0层的sstable文件数量达到此值, 需要做一次休眠一微秒的操作
	defaultSlowDownTrigger = 8

	// 写入的时候进行检查, 如果level0成的sstable文件数量达到此值, 需要直接做compaction操作
	defaultPauseTrigger = 12

	// 默认当level[0]跟level[1]无重复, 并且level[0]跟gp层重复的文件数量不超过该值, 则可以直接将
	// level[0]直接放置到level[1]
	defaultTrivialGpLimitFiles = 10

	// 默认下一层是上一层size的10倍
	defaultCompactionTotalSizeMulter = 10

	// 默认基础层的总大小
	defaultLevelTotalSize = 10 * mb
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

func (opt *Options) GetCompactionLimit() int64 {
	return defaultCompactionLimitFiles * defaultSStableFileSize
}

func (opt *Options) GetCompactionTrivialGpFiles() int {
	return defaultTrivialGpLimitFiles
}

func (opt *Options) GetTableFileSize() int {
	return defaultSStableFileSize
}

func (opt *Options) GetLevel0TriggerLen() int {
	return defaultPauseTrigger
}

func (opt *Options) GetCompactionSizeLevel(level int) int64 {
	multer := math.Pow(defaultCompactionTotalSizeMulter, float64(level))
	return int64(multer) * defaultLevelTotalSize
}
