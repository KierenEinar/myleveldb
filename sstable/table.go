package sstable

import "encoding/binary"

/**
sstable 简介

一个完整的sstable文件的数据结构是这样的
可以把 index block 看成是一个一级索引, data block 里边的restart point 是一个二级索引
																						  48Byte
																						/		\

	+---------------+---------------+---------------+------------------+---------------+----------+
	| data block 0  | data block n  | filter block	| meta index block |  index block  |  footer  |
	+---------------+---------------+---------------+------------------+---------------+----------+

一个block的数据结构是这样的, compression type: 1byte, check sum: 4byte
写入一个完整的block数据后, 会返回offset, length(当前data的长度减去checksum的4个字节和压缩类型的一个字节)



    +-----------------------------------------------------+------------------------+---------------+
	|			  			  data			     	      |	   compression type    |   check sum   |
	+-----------------------------------------------------+------------------------+---------------+

data的数据结构是这样的

	 restart point 0                 restart point 1
	/                               /
	+-------------------------------+-----------------------------+-----------+----------+---------+
	| 			block entry	    	|		   block entry   	  | rs index0 | rs index1|  rs len |
	+-------------------------------+-----------------------------+-----------+----------+---------+

block entry的数据结构是这样的

								entry 1												  entry n
	/																	\		/				  \
	+---------------+------------------+-----------+-------------+-------+-----+-------------------+
	| share key len | un share key len | value len | unshare key | value | ... |				   |
	+---------------+------------------+-----------+-------------+-------+-----+-------------------+

一般来说, data block的一个block entry 包含16个kv对
filter block, meta index block, index block 都是一个block entry 1个kv对

filter block的数据结构是这样的

	/ offset 0		  / offset 1        / offset n        / offsets offset
	+-----------------+-----------------+-----------------+---------+---------+---------+---------+--------+
	| 	filter0 data  |   filter1 data  |   filtern data  | of0 idx | of1 idx | ofn idx | of ofset| base lg|
	+-----------------+-----------------+-----------------+---------+---------+---------+---------+--------+
filter block
写入规则:
	默认base lg是11, 也就是当一个data block写满的时候，会判断当前data block的最后一个offset
	是否满足一个filter data的大小, 若是则将当前内存中的所有keys, 生成filter data的字节数组
查找规则:
	查找当前的key在哪个index block, 通过index block的value获取到offset和length,
	查询命中到的data block究竟在filter中属于哪个 [offset, offset+1], 通过filter data 查询到真正的data


meta index block
仅包含一个block entry, key是 filter.{filtername}, value是filter block对应的block handle {offset, length}

	/					block entry					 \							/      block tail			\
	+-----------+--------------+-------+-----+-------+--------------+-----------+-----------+------------+
	| shr k len | un shr k len | v len | key | value | rs point idx | 	rs len  | comp type | check sum	 |
	+-----------+--------------+-------+-----+-------+--------------+-----------+-----------+------------+

index block
index block 每一个kv对都是针对data block而言的
	举例, data0 block的最后一个key是hello, offset和length分别是0, 32
	data1 block的第一个key是 hellx, 最后一个key是world, offset和length分别是32, 88
	datan block的第一个key是 www, 最后一个key是xxx, offset和length 分别是 3k, 36
	还有一个小点, 仅当下一个data block的key是空的时候, index block的key才是首字母直接ascii码+1,
	其余的时候, 比较的是data0 block的最后一个字母和data1 block的第一个字母的共同字母的个数,
	然后在data0 block比较后的下一个字母ascii码+1+1

	/    key   \/  value  \/   key   \/  value  \/   key   \/  value  \
	+----------+----------+----------+----------+----------+----------+--------+--------+--------+
	|   hellp  | 0, 32    |    wp    |  32, 88  |     y    |  3k, 36  | rs0 idx| rs1 idx| rsn idx|
	+----------+----------+----------+----------+----------+----------+--------+--------+--------+

查找一个key的过程
	先定位index block, 找出第一个大于或等于当前key的idx, 那么idx-1则是要找的data block, 有可能key不在idx-1,
	但是在idx的 data block, 所以当定位不到的时候需要往下一个data block的首个kv对的key中查找, 注(可以先用bloom filter判断当前key是否在集合中)
	有可能最后一个key是 abcd, 下一个block的key是abce, 那么index block data的key就是abce, 所以需要向下一个block查找


	定位data block的时候要先从restart point中查找第一个大于当前key的index, 那么index-1则是数据所在区域。再seek即可。

footer的结构, 48个字节

	/	meta index block handle \/	index block handle	\/    padding   \/	   magic    \
	+---------------------------+-----------------------+---------------+---------------+
	|		varint			 	|		varint			|			    |	  8byte	    |
	+---------------------------+-----------------------+---------------+---------------+

magic: The magic are first 64-bit of SHA-1 sum of "http://code.google.com/p/leveldb/".

注: 所有的字节存储方式均是小端序

**/

const (
	magic                      = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"
	footerLength               = 48
	blockTrialLen              = 5
	blockTypeNoCompression     = 0
	blockTypeSnappyCompression = 1
)

type blockHandle struct {
	offset uint64
	length uint64
}

func decodeBlockHandle(src []byte) (*blockHandle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	return &blockHandle{
		offset: offset,
		length: length,
	}, n + m
}

func encodeBlockHandle(dst []byte, bh blockHandle) int {
	n := binary.PutUvarint(dst, bh.offset)
	m := binary.PutUvarint(dst[n:], bh.length)
	return n + m
}
