package journal

import "errors"

/***

journal 说明

格式

	journal 格式

	journal = block * n

	/---------------/---------------/---------------/---------------/---------------/---------------/
	|	  block		|				|				|				|				|				|
	/---------------/---------------/---------------/---------------/---------------/---------------/

	block = m * record

	record 格式

    /   4 字节的check sum   / 2字节的length / 1字节的type /
	/-------------------------------------------------/-----------------------------------------------------/
	|    			   record header	              |						  record						|
	/-------------------------------------------------/-----------------------------------------------------/

chunk -> 指的是写入的一个完整数据
block -> 一个journal文件每32kb组成一个块
record -> block的写入单位
*/

const (
	recordTypeFull   = 1 // 表明当前的数据在这个record是完整的
	recordTypeFirst  = 2 // 表明当前的数据在这个record不是完整的，是第一个chunk
	recordTypeMiddle = 3 // 表明当前的数据在这个record不是完整的，是中间的chunk
	recordTypeLast   = 4 // 表明当前的数据在这个record不是完整的，是最后的chunk
)

const (
	headerSize = 7
	blockSize  = 32 * (1 << 10)
)

var (
	ErrSkipped = errors.New("err read record skipped")
)
