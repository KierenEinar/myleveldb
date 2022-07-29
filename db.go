package myleveldb

import (
	"myleveldb/journal"
	"myleveldb/memdb"
	"myleveldb/storage"
	"myleveldb/utils"
	"os"
	"sync"
	"sync/atomic"
)

// DB 数据库
type DB struct {
	seq uint64 // 时序
	s   *Session

	pool *utils.BytePool

	// journal 相关
	journalFd     storage.FileDesc
	journal       *journal.Writer
	journalWriter storage.Writer

	memMu       sync.Mutex
	memDb       *memdb.MemDB // 内存数据库
	frozenMemDb *memdb.MemDB // 只能被读取
	// 冻结manifest相关
	frozenJournalFd storage.FileDesc
	frozenSeq       uint64

	writeMerge *WriteMerge
	withBatch  *WithBatch

	// compaction相关
	mcompCmdC  chan cCmd
	tcompCmdC  chan cCmd
	tPauseCmdC chan chan struct{} // 正在执行compaction的暂停指令

	closeC chan struct{}
}

func (db *DB) addSeq(delta uint64) {
	atomic.AddUint64(&db.seq, delta)
}

// Open 打开数据库
func Open(filepath string, opt *Options) (*DB, error) {

	stor, err := storage.OpenFile(filepath, false)
	if err != nil {
		return nil, err
	}

	session, err := newSession(stor, opt)
	if err != nil {
		return nil, err
	}

	// 恢复manifest的信息到session中
	err = session.recover()
	if err != nil {

		if err != os.ErrNotExist {
			return nil, err
		}

		err = session.create() // 首次打开不存在manifest文件, 创建一个
		if err != nil {
			return nil, err
		}
	}

	return openDB(session)

}

// 加载db
func openDB(s *Session) (*DB, error) {

	db := &DB{
		seq:        s.stSeqNum,
		s:          s,
		pool:       s.Options.GetPool(),
		writeMerge: NewWriteMerge(),
		mcompCmdC:  make(chan cCmd),
		tcompCmdC:  make(chan cCmd),
		tPauseCmdC: make(chan chan struct{}),
	}

	db.withBatch = &WithBatch{
		makeRoomForWrite: db.makeRoomForWrite,
		writeBatch:       db.writeBatchLocked,
	}

	err := db.recoverJournal()
	if err != nil {
		return nil, err
	}

	// todo 清理掉不必要的文件

	// 开启memdb compaction
	go db.mCompaction()

	return db, nil
}
