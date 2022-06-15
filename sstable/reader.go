package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"myleveldb/cache"
	"myleveldb/collections"
	"myleveldb/comparer"
	"myleveldb/filter"
	"myleveldb/iter"
	"myleveldb/utils"
	"sort"
)

var (
	ErrBlockHandle           = errors.New("block handle decode err")
	ErrBlockOffsetNotAligned = errors.New("offset not aligned")
	ErrDataBlockDecode       = errors.New("data block decode err")
	ErrDataBlockCheckSum     = errors.New("data block check sum err")
	ErrCompressTypeUnsupport = errors.New("data block compress not support err")
	ErrFooterMagic           = errors.New("footer magic err")
	ErrNotFound              = errors.New("not found err")
)

// 封装了对data block的 seek操作和获取entry
type dataBlock struct {
	restartsOffset int
	restartsLen    int
	data           []byte
	bytePool       *utils.BytePool
	cmp            comparer.Compare
}

func newDataBlock(data []byte, bytePool *utils.BytePool, cmp comparer.Compare) *dataBlock {
	restartsNum := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartsOffset := len(data) - (restartsNum+1)*4
	return &dataBlock{
		restartsOffset: restartsOffset,
		restartsLen:    restartsNum,
		data:           data,
		bytePool:       bytePool,
		cmp:            cmp,
	}
}

// 根据给定的key, 找出第一个 restart point对应下标的key 大于给定的key的值
// 然后对找出来的restart point做一次 减1操作, 因为只有该值往后寻找可能存在大于或等于给定的key
func (block *dataBlock) seekRestartPoint(key []byte) (offset, index int) {
	index = sort.Search(block.restartsLen, func(i int) bool {
		offset = int(binary.LittleEndian.Uint32(block.data[block.restartsOffset+i*4:]))
		offset++
		nKey, n := binary.Uvarint(block.data[offset:]) // 未shareprefix 的key长度, 和占用的字节大小
		_, m1 := binary.Uvarint(block.data[offset+n:]) // 获取value len的长度
		restartKey := block.data[offset+n+m1 : offset+n+m1+int(nKey)]
		return block.cmp(restartKey, key) > 1
	})

	if index > 0 {
		index--
	}

	offset = int(binary.LittleEndian.Uint32(block.data[block.restartsOffset+index*4:]))
	return
}

func (block *dataBlock) entry(offset int) (unShareKey, value []byte, nShared, n int, err error) {

	if offset >= block.restartsOffset {
		if offset != block.restartsOffset {
			err = ErrBlockOffsetNotAligned
		}
		return
	}

	v0, n0 := binary.Uvarint(block.data[offset:])       // share key len
	v1, n1 := binary.Uvarint(block.data[offset+n0:])    // unshare key len
	v2, n2 := binary.Uvarint(block.data[offset+n0+n1:]) // value len
	n = n0 + n1 + n2 + int(v1) + int(v2)
	unShareKey = block.data[offset+n0+n1+n2 : offset+n0+n1+n2+int(v1)]
	value = block.data[offset+n0+n1+n2+int(v1) : offset+n]
	nShared = int(v0)
	return
}

func (block *dataBlock) UnRef() {
	if block.bytePool != nil {
		block.bytePool.Put(block.data)
	}
}

// FilterBlock filter block的内容相关
type FilterBlock struct {
	bloomFilter filter.IFilter
	pool        *utils.BytePool
	data        []byte // 具体的内容
	baseLg      uint8  // filter block 分组的分子
	oOffset     int    // filter block offset的 position
	filterNums  int    // 总共有几个分组
}

func (filterBlock *FilterBlock) contains(offset int, key []byte) bool {
	segment := offset >> filterBlock.baseLg // 获取是在第几个区间
	if segment < filterBlock.filterNums {
		filter := filterBlock.bloomFilter
		o := filterBlock.data[filterBlock.oOffset+segment*4:]
		m := binary.LittleEndian.Uint32(o)     // 获取区间开始的真正下标
		n := binary.LittleEndian.Uint32(o[4:]) // 获取区间结束的真正下标, 如果当前不够一个segment, 那么获取到的下标是offset的下标
		return filter.Contains(filterBlock.data[m:n], key)
	}
	return true
}

func (filterBlock *FilterBlock) UnRef() {
	filterBlock.pool.Put(filterBlock.data)
}

// BlockIter 封装了对data block的相关遍历操作
type BlockIter struct {
	dataBlock *dataBlock

	blockReleaser utils.Releaser

	// 遍历相关
	offset       int // 当前下标所处的位置
	restartIndex int // 当前正在哪个restart point

	// 当前遍历的key, value
	key, value []byte

	// soi 代表是否开始遍历
	// eoi 代表是否结束遍历
	soi, eoi bool

	// 是否释放掉
	released bool

	err error
}

func newBlockIter(dataBlock *dataBlock, blockReleaser utils.Releaser) *BlockIter {

	return &BlockIter{
		dataBlock:     dataBlock,
		blockReleaser: blockReleaser,
		soi:           true,
		eoi:           false,
		err:           nil,
	}

}

func (r *Reader) getDataIter(bh blockHandle) (iter.Iterator, error) {
	block, rel, err := r.readBlockCached(bh)
	if err != nil {
		return nil, err
	}
	return newBlockIter(block, rel), nil
}

// Seek 寻找大于等于key的下标
func (bi *BlockIter) Seek(key []byte) bool {

	bi.soi = false
	bi.eoi = false

	// 按照restart point定位block entry
	offset, restartIndex := bi.dataBlock.seekRestartPoint(key)

	// 第一个block entry是没有共享前缀的

	bi.offset = offset
	bi.restartIndex = restartIndex

	for bi.Next() {
		if bi.dataBlock.cmp(bi.key, key) >= 0 {
			return true
		}
	}

	return false

}

func (bi *BlockIter) Next() bool {

	if bi.eoi {
		return false
	}

	if bi.soi {
		bi.soi = false
	}

	unShareKey, value, nShared, n, err := bi.dataBlock.entry(bi.offset)
	if err != nil {
		bi.err = err
		return false
	}
	if n == 0 {
		bi.eoi = true
		return false
	}
	bi.key = append(bi.key[:nShared], unShareKey...)
	bi.value = value
	bi.offset += n
	return true
}

func (bi *BlockIter) Key() []byte {
	if bi.soi || bi.eoi {
		return nil
	}
	return bi.key
}

func (bi *BlockIter) Value() []byte {
	if bi.soi || bi.eoi {
		return nil
	}
	return bi.value
}

func (bi *BlockIter) UnRef() {
	if !bi.released {
		bi.released = true
		if bi.blockReleaser != nil {
			bi.blockReleaser.UnRef()
		}
	}
}

// Reader reader读取操作
type Reader struct {
	reader io.ReaderAt // 随机读的io

	// sstable的 block handle
	metaBH, indexBH, metaIndexBH blockHandle

	// indexblock
	indexBlock  *dataBlock
	filterBlock *FilterBlock

	filter filter.IFilter
	// cmp
	cmp comparer.Compare

	// 字节数组池对象
	bytePool *utils.BytePool
	cache    *cache.NamespaceCache
}

func (r *Reader) readBlockCached(bh blockHandle) (*dataBlock, utils.Releaser, error) {

	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, bh.offset)
	ch, err := r.cache.Get(key, func() (int64, collections.Value, collections.BucketNodeDeleterCallback, error) {
		dataBlock, err := r.readBlock(bh, true)
		if err != nil {
			return 0, nil, nil, err
		}
		return int64(cap(dataBlock.data)), dataBlock, nil, nil
	})
	if err != nil {
		return nil, nil, err
	}

	block := ch.Value().(*dataBlock)
	return block, ch, nil
}

func (r *Reader) readBlock(bh blockHandle, verifyCheckSum bool) (*dataBlock, error) {
	data, err := r.readRawBlock(bh, verifyCheckSum)
	if err != nil {
		return nil, err
	}
	return newDataBlock(data, r.bytePool, r.cmp), nil
}

func (r *Reader) readRawBlock(bh blockHandle, verifyCheckSum bool) (data []byte, err error) {

	data = r.bytePool.Get(int64(bh.length + blockTrialLen))

	defer func() {
		if err != nil {
			r.bytePool.Put(data)
		}
	}()

	_, err = r.reader.ReadAt(data, int64(bh.offset))
	if err != nil {
		return nil, err
	}

	if len(data) < 5 {
		return nil, ErrDataBlockDecode
	}

	if verifyCheckSum {
		checkSum1 := binary.LittleEndian.Uint32(data[len(data)-4:]) // 获取已经计算好的
		checkSum2 := crc32.ChecksumIEEE(data[:len(data)-4])         // 重新计算一遍
		if checkSum1 != checkSum2 {
			return nil, ErrDataBlockCheckSum
		}
	}

	// 获取压缩类型
	ct := data[len(data)-5]

	switch compressionType(ct) {
	case noCompress:
		return data[:len(data)-4], nil
	default:
		return nil, ErrCompressTypeUnsupport
	}

}

func (r *Reader) readFilterBlock(bh blockHandle) (*FilterBlock, error) {
	data, err := r.readRawBlock(bh, true)
	if err != nil {
		r.bytePool.Put(data)
		return nil, err
	}

	baseLg := data[len(data)-1]
	oOffset := binary.LittleEndian.Uint32(data[len(data)-5:])
	filtersNum := (len(data) - 5 - int(oOffset)) / 4

	return &FilterBlock{
		bloomFilter: r.filter,
		pool:        r.bytePool,
		data:        data,
		baseLg:      baseLg,
		oOffset:     int(oOffset),
		filterNums:  filtersNum,
	}, nil

}

func (r *Reader) readFilterBlockCached(bh blockHandle) (block *FilterBlock, releaser utils.Releaser, err error) {
	key := r.bytePool.Get(8)
	binary.LittleEndian.PutUint64(key, bh.offset)
	defer func() {
		if err != nil {
			r.bytePool.Put(key)
		}
	}()

	ch, err := r.cache.Get(key, func() (int64, collections.Value, collections.BucketNodeDeleterCallback, error) {
		block, err = r.readFilterBlock(bh)
		if err != nil {
			return 0, nil, nil, err
		}
		return int64(cap(block.data)), block, nil, nil
	})
	return block, ch, nil
}

// 寻找第一个大于或者等于key的值
func (r *Reader) find(key []byte, filtered, noValue bool) (rkey []byte, rvalue []byte, err error) {

	/**
	搜索流程

	|-------------|--------------|--------------|--------------
	           abc abcdef     bbb  bbz        cc  dfg


	/ offset0 / offset1 / offset2
	--------- --------- ---------
	|  abc  | |  bbc  | |   cc   |
	--------- --------- ---------
	  0 100    100 120    220 50

	---------
	|  abc  |  0 100
	---------
	---------
	|  bbc  |  100 120
	---------
	---------
	|   d   |  220 50
	---------

	搜abc  落在 0 100, abc >= 'abc', 所以在第一个data block
	搜abcd 落在 100 200, bbc >= 'abcd', 所以在第二个data block
	搜abce 落在 100 100, bbc >= 'abce', 所以在第二个data block
	搜bba  落在 100 200, bbc >= 'bba', 所以在第二个data block
	搜bbd  落在 220 270, d >= 'bbd', 所以在第二个data block
	搜bbc  落在 100 220, bbc >= 'bbc', 所以在第二个data block, 但是实际上它不在第二块,
	应该是在第三块中(所以找不到时仍然需要找下一个data block的), 也说明了filter block可能需要两段查找才是最精确的

	*/

	// 获取index block
	indexIter, err := r.getDataIter(r.indexBH)
	if err != nil {
		return nil, nil, err
	}
	// 解引用
	defer indexIter.UnRef()

	// 通过index block 去寻找第一个大于或者等于key的值, 通过其blockhandle定位到data block所在的位置
	if !indexIter.Seek(key) {
		return nil, nil, ErrNotFound
	}

	blockHandle, n := decodeBlockHandle(indexIter.Value())
	if n == 0 {
		return nil, nil, ErrBlockHandle
	}

	if filtered {
		// 获取bloom filter
		filterBlock, rel, err := r.readFilterBlockCached(r.metaBH)
		if err != nil {
			return nil, nil, err
		}
		defer rel.UnRef()

		// bloom filter查找key是否存在
		if !filterBlock.contains(int(blockHandle.offset), key) {
			return nil, nil, ErrNotFound
		}
	}

	// 获取数据所在的data block
	dataIter, err := r.getDataIter(blockHandle)
	if err != nil {
		return nil, nil, err
	}
	defer dataIter.UnRef()

	if !dataIter.Seek(key) {
		// 如果已经是最后一个data block了,
		if !indexIter.Next() {
			return nil, nil, ErrNotFound
		}

		blockHandle, n = decodeBlockHandle(indexIter.Value())
		if n == 0 {
			return nil, nil, ErrBlockHandle
		}

		dataIter1, err := r.getDataIter(blockHandle)
		if err != nil {
			return nil, nil, err
		}

		defer dataIter1.UnRef()

	}

	rkey = dataIter.Key()
	if !noValue {
		rvalue = dataIter.Value()
	}
	return
}

// FindKey 搜寻sstable中>=key的最小key
func (r *Reader) FindKey(key []byte) (rkey []byte, err error) {
	rkey, _, err = r.find(key, false, false)
	return
}

// Find 搜寻sstable中>=key的最小key value pair
func (r *Reader) Find(key []byte) (rkey []byte, value []byte, err error) {
	rkey, value, err = r.find(key, false, false)
	return
}

// Get 通过key获取value
func (r *Reader) Get(key []byte) (value []byte, err error) {

	rkey, value, err := r.find(key, false, false)
	if err != nil {
		return nil, err
	}

	if r.cmp(rkey, key) != 0 {
		value = nil
		err = ErrNotFound
	}
	return
}

// NewReader 新建一个sstable 的reader对象
func NewReader(readFd io.ReaderAt, size int64, cmp comparer.Compare,
	nsCache *cache.NamespaceCache) (*Reader, error) {

	if size < footerLength {
		return nil, ErrDataBlockDecode
	}

	r := &Reader{
		reader: readFd,
		cache:  nsCache,
		cmp:    cmp,
	}

	// 读取尾部
	footer := make([]byte, footerLength)
	_, err := readFd.ReadAt(footer, size-footerLength)
	if err != nil {
		return nil, err
	}

	if utils.BytesToString(footer[footerLength-8:]) != magic {
		return nil, ErrFooterMagic
	}

	var n int

	// 读取meta index bh
	r.metaIndexBH, n = decodeBlockHandle(footer)

	// 读取index block bh
	r.indexBH, _ = decodeBlockHandle(footer[n:])

	// 解析meta index block
	metaIndexBlock, err := r.readBlock(r.metaIndexBH, true)
	if err != nil {
		return nil, err
	}

	metaIndexIter := newBlockIter(metaIndexBlock, nil)

	for metaIndexIter.Next() {
		key := metaIndexIter.Key()
		if !bytes.HasPrefix(key, []byte("filter.")) {
			continue
		}
		bh := metaIndexIter.Value()
		r.metaBH, n = decodeBlockHandle(bh)
	}

	metaIndexBlock.UnRef()
	metaIndexIter.UnRef()

	return r, nil

}
