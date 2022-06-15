package filter

import (
	"bytes"
	"hash/fnv"
	"math"
)

/**
bloom filter简介

						/		/				key2			   \\\\

				//     /        key1              \\\\
	+--------+--------+--------+--------+--------+--------+--------+--------+
	|00000000|01110110|10000000|10000001|00000000|11110000|00000001|11100000|
	+--------+--------+--------+--------+--------+--------+--------+--------+

key 经过k个hash函数散列后(每次hash函数会得出一个值, 然后对m取模, 得出所在bit的位置), 经过k个函数后, 将得出的位置设置为1
所以根据该特点，可以得出如果一个key输入, 经过k个hash函数后, 所得出的k个bit的下标如果存在0的话, 那么该key铁定不在集合内
反之无法推断

数学背景有点难, 只要记住一个已经推断好的事实即可,
假设一个key用n个bits表示, 那么最佳的k个hash函数的选择为 n * 0.69 , e.g. 一个key用10个bits表示, 那么最佳的k是7个

**/

var (
	hashFnv32 = fnv.New32()
)

const (
	defaultBitsPerKey = 10
	maxBitsPerKey     = 30
)

// BloomFilter 布隆过滤器
type BloomFilter struct{}

// NewFilterGenerator 实例化一个过滤器生成器
func (f *BloomFilter) NewFilterGenerator(bitsPerKey uint8) IFilterGenerator {
	if bitsPerKey <= 0 {
		bitsPerKey = defaultBitsPerKey
	}

	if bitsPerKey > maxBitsPerKey {
		bitsPerKey = maxBitsPerKey
	}

	k := calculateNumOfHashFunc(bitsPerKey)
	return &BloomFilterGenerator{
		bitsPerKey: bitsPerKey,
		k:          k,
	}
}

// Contains 当前布隆过滤器是包含在有key
func (filter *BloomFilter) Contains(data []byte, key []byte) bool {
	nBytes := len(data) - 1
	if nBytes <= 1 {
		panic("bloom filter buf input error")
	}
	k := data[nBytes]
	nBits := nBytes * 8
	hash := bloomHash(key)
	delta := (hash >> 17) | (hash << 15)
	for i := uint8(0); i < k; i++ {
		pos := hash % uint32(nBits)
		nBytePos := pos / 8
		if (data[nBytePos] & (1 << (pos % 8))) == 0 {
			return false
		}
		hash += delta
	}
	return true
}

// Name 名称
func (filter *BloomFilter) Name() string {
	return "bloomfilter"
}

// BloomFilterGenerator 布隆过滤器生成器
type BloomFilterGenerator struct {
	bitsPerKey uint8
	keyHashes  []uint32
	k          uint8
}

// Add 将key 加入布隆过滤器集合
func (filter *BloomFilterGenerator) Add(key []byte) {
	filter.keyHashes = append(filter.keyHashes, bloomHash(key))
}

// Generate 将keyHashes解析成bits, 并写入到buffer中
func (filter *BloomFilterGenerator) Generate(buf *bytes.Buffer) {

	keys := len(filter.keyHashes)
	// 算出需要多少位
	nBits := keys * int(filter.bitsPerKey)

	// 当bits特别少的时候, 错误率较高, 需要修正bits最小64
	if nBits < 64 {
		nBits = 64
	}

	// 换算出需要多少字节
	nBytes := (nBits + 7) / 8

	// 计算真正的bits
	nBits = nBytes * 8

	data := make([]byte, nBytes+1)

	// 将所有hash函数取出来
	for _, hash := range filter.keyHashes {
		delta := (hash >> 17) | (hash << 15)
		for i := uint8(0); i < filter.k; i++ {
			pos := hash % uint32(nBits)
			nBytePos := pos / 8
			data[nBytePos] |= (1 << (pos % 8))
			hash += delta
			// fmt.Printf("%d-%d\n", nBytePos, pos%8)
		}
		// fmt.Println("-------------")
	}

	data[nBytes] = filter.k

	buf.Write(data)
}

func calculateNumOfHashFunc(bitsPerKey uint8) uint8 {
	return uint8(math.Ceil(0.69 * float64(bitsPerKey)))
}

func bloomHash(key []byte) uint32 {
	return fnv32(key)
}

func fnv32(key []byte) uint32 {
	_, _ = hashFnv32.Write(key)
	defer hashFnv32.Reset()
	return hashFnv32.Sum32()
}
