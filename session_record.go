package myleveldb

import (
	"encoding/binary"
	"errors"
	"io"
)

/***

session_record 等同于 version_edit
多个session_record通过增量计算, 可以推断出数据库的最终状态(session)

CURRENT的内容为MANIFEST的文件

MANIFEST的文件是通过journal格式写和读的, 不同于log, MANIFEST直接写入, 跟log格式(chunk)不同,
不会存在batch_data(前8位为seq, 中间4位为batch_entry的数量, 随后就存data)

MANIFEST第一次读取的session_record为snap_shot, 随后的每一次读取session_record都为delta增量计算

session_record的格式如下

key: 类型
value: 类型不同, value不同

key类型枚举
数字

value类型枚举
数字
byte数组, 前面varint存byte的长度, 后边带byte数据

1-4, 数据库运行期间依赖的数据, 比较器, 日志num, 下一个可用文件的num, 序列号
5-7, 数据库运行期间compaction的数据, level下一个compact的key, level删除的文件, level新增的文件
9, prev日志, 废弃
*比较器名称
key: 1
key类型: varint
value: comparator的名称
value类型: []byte


*当前对应的日志文件num
key: 2
key类型: varint
value: 日志文件对应的num
value类型: varint


*下一个可用的文件num
key: 3
key类型: varint
value: 日志文件对应的num
value类型: varint


*当前数据库的序列号sequence
key: 4
key类型: varint
value: seq
value类型: varint

type cpRecord struct {
	cLevel int
	cKey   internalKey
}

*level对应的下一个待合并的key
key: 5
key类型: varint
value: cpRecord
value类型: []byte


type dlRecord struct {
	level int
	num int
}

*level对应的ldb删除文件的编号
key: 6
key类型: varint
value: dlRecord
value类型: []byte

type atRecord struct {
	level int
	num int
	size int
	min, max internalKey
}

*level对应的ldb新增文件
key: 7
key类型: varint
value: atRecord
value类型: []byte

*prevJournalNum 已弃用
key: 9
key类型: varint
value: int
value类型: varint

**/

// 以下常量不能被变更, 会写入到文件系统中
const (
	recComparer       = 1
	recJournalNum     = 2
	recNextFileNum    = 3
	recSequenceNum    = 4
	recCompactPtr     = 5
	recDelRecord      = 6
	recAddRecord      = 7
	recPrevJournalNum = 9 // 已废弃
)

var (
	ErrCorupted = errors.New("corupted content ack")
)

type Reader interface {
	io.Reader
	io.ByteReader
}

type compactPtr struct {
	cLevel int
	cKey   internalKey
}

type dlRecord struct {
	level int
	num   int
}

type atRecord struct {
	level    int
	num      int
	size     int
	min, max internalKey
}

// SessionRecord version edit
type SessionRecord struct {
	hasRec         int64
	comparer       []byte
	journalNum     int64
	nextFileNum    int64
	sequenceNum    uint64
	compactPtrs    []compactPtr
	dlRecords      []dlRecord
	atRecords      []atRecord
	prevJournalNum int64
	scratch        [binary.MaxVarintLen64]byte
	err            error
}

func (p *SessionRecord) putUVarInt(writer io.Writer, u uint64) {

	if p.err != nil {
		return
	}

	n := binary.PutUvarint(p.scratch[:], u)
	_, err := writer.Write(p.scratch[:n])
	if err != nil {
		p.err = err
		return
	}
	return
}

func (p *SessionRecord) putVarInt(writer io.Writer, u int64) {

	if u < 0 {
		p.err = errors.New("negative int64 value")
		return
	}

	p.putUVarInt(writer, uint64(u))
}

func (p *SessionRecord) putBytes(writer io.Writer, data []byte) {

	if p.err != nil {
		return
	}

	p.putVarInt(writer, int64(len(data)))

	_, err := writer.Write(data)
	if err != nil {
		p.err = err
	}
	return
}

func (p *SessionRecord) setComparer(cmp []byte) {
	p.hasRec |= 1 << recComparer
	p.comparer = cmp
}

func (p *SessionRecord) setJournalNum(journalNum int64) {
	p.hasRec |= 1 << recJournalNum
	p.journalNum = journalNum
}

func (p *SessionRecord) setNextFileNum(nextFileNum int64) {
	p.hasRec |= 1 << recNextFileNum
	p.nextFileNum = nextFileNum
}

func (p *SessionRecord) setSequenceNum(sequenceNum uint64) {
	p.hasRec |= 1 << recSequenceNum
	p.sequenceNum = sequenceNum
}

func (p *SessionRecord) addCompactPtr(cptr compactPtr) {
	p.hasRec |= 1 << recCompactPtr
	p.compactPtrs = append(p.compactPtrs, cptr)
}

func (p *SessionRecord) delRecord(record dlRecord) {
	p.hasRec |= 1 << recDelRecord
	p.dlRecords = append(p.dlRecords, record)
}

func (p *SessionRecord) addRecord(record atRecord) {
	p.hasRec |= 1 << recAddRecord
	p.atRecords = append(p.atRecords, record)
}

func (p *SessionRecord) addTableFile(level int, file tFile) {

	at := atRecord{
		level: level,
		num:   file.fd.Num,
		size:  int(file.size),
		min:   file.min,
		max:   file.max,
	}
	p.addRecord(at)
}

func (p *SessionRecord) delTableFile(level int, file tFile) {
	dl := dlRecord{
		level: level,
		num:   file.fd.Num,
	}
	p.delRecord(dl)
}

func (p *SessionRecord) hasField(rec int) bool {
	return (p.hasRec & 1 << rec) != 0
}

func (p *SessionRecord) encode(writer io.Writer) (err error) {

	if p.hasField(recComparer) {
		p.putUVarInt(writer, recComparer)
		p.putBytes(writer, p.comparer)
	}

	if p.hasField(recJournalNum) {
		p.putUVarInt(writer, recJournalNum)
		p.putVarInt(writer, p.journalNum)
	}

	if p.hasField(recNextFileNum) {
		p.putUVarInt(writer, recNextFileNum)
		p.putVarInt(writer, p.nextFileNum)
	}

	if p.hasField(recSequenceNum) {
		p.putUVarInt(writer, recSequenceNum)
		p.putUVarInt(writer, p.sequenceNum)
	}

	if p.hasField(recCompactPtr) {
		for _, v := range p.compactPtrs {
			p.putUVarInt(writer, recCompactPtr)
			p.putVarInt(writer, int64(v.cLevel))
			p.putBytes(writer, v.cKey)
		}
	}

	if p.hasField(recDelRecord) {
		for _, v := range p.dlRecords {
			p.putUVarInt(writer, recDelRecord)
			p.putVarInt(writer, int64(v.level))
			p.putVarInt(writer, int64(v.num))
		}
	}

	if p.hasField(recAddRecord) {
		for _, v := range p.atRecords {
			p.putUVarInt(writer, recAddRecord)
			p.putVarInt(writer, int64(v.level))
			p.putVarInt(writer, int64(v.num))
			p.putVarInt(writer, int64(v.size))
			p.putBytes(writer, v.min)
			p.putBytes(writer, v.max)
		}
	}

	if p.err != nil {
		return p.err
	}
	return nil
}

func (p *SessionRecord) readUVarInt(r Reader) uint64 {
	return p.readUVarIntMayEOF(r, false)
}

func (p *SessionRecord) readVarInt(r Reader) int64 {
	u := p.readUVarIntMayEOF(r, false)
	if p.err != nil {
		return 0
	}
	if int64(u) < 0 {
		p.err = ErrCorupted
		return 0
	}
	return int64(u)
}

func (p *SessionRecord) readBytes(r Reader) []byte {

	l := p.readUVarIntMayEOF(r, false)
	if p.err != nil {
		return nil
	}

	data := make([]byte, l)
	_, err := io.ReadFull(r, data)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			p.err = ErrCorupted
		}
		return nil
	}

	return data
}

func (p *SessionRecord) readUVarIntMayEOF(r Reader, mayEOF bool) uint64 {

	if p.err != nil {
		return 0
	}

	uVarInt, err := binary.ReadUvarint(r)
	if err != nil {
		if err == io.ErrUnexpectedEOF || (mayEOF == false && err == io.EOF) {
			p.err = ErrCorupted
			return 0
		}
		p.err = err
		return 0
	}

	return uVarInt

}

func (p *SessionRecord) decode(r Reader) (err error) {

	for {
		rec := p.readUVarIntMayEOF(r, true)
		if p.err != nil {
			if p.err == io.EOF {
				return
			}
			return p.err
		}

		p.hasRec |= 1 << rec

		switch rec {
		case recComparer:
			p.comparer = p.readBytes(r)
		case recJournalNum:
			p.journalNum = p.readVarInt(r)
		case recNextFileNum:
			p.nextFileNum = p.readVarInt(r)
		case recSequenceNum:
			p.sequenceNum = p.readUVarInt(r)
		case recCompactPtr:
			level := p.readVarInt(r)
			cKey := p.readBytes(r)

			p.compactPtrs = append(p.compactPtrs, compactPtr{
				cLevel: int(level),
				cKey:   cKey,
			})

		case recDelRecord:

			level := p.readVarInt(r)
			num := p.readVarInt(r)
			p.dlRecords = append(p.dlRecords, dlRecord{
				level: int(level),
				num:   int(num),
			})
		case recAddRecord:

			level := p.readVarInt(r)
			num := p.readVarInt(r)
			size := p.readVarInt(r)
			min := p.readBytes(r)
			max := p.readBytes(r)

			p.atRecords = append(p.atRecords, atRecord{
				level: int(level),
				num:   int(num),
				size:  int(size),
				min:   min,
				max:   max,
			})
		}

		if p.err != nil {
			return p.err
		}
	}

}

func (p *SessionRecord) resetAddRecord() {
	p.atRecords = p.atRecords[:0]
}

func (p *SessionRecord) resetDelRecord() {
	p.dlRecords = p.dlRecords[:0]
}

func (p *SessionRecord) resetCompatPtr() {
	p.compactPtrs = p.compactPtrs[:0]
}
