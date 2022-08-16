package myleveldb

import (
	"myleveldb/collections"
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
	tPauseCmdC chan chan<- struct{} // 正在执行compaction的暂停指令

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
		tPauseCmdC: make(chan chan<- struct{}),
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

func (db *DB) Put(key, value []byte) error {
	return db.putRec(key, value, keyTypeVal)
}

func (db *DB) Delete(key []byte) error {
	return db.putRec(key, nil, keyTypeDel)
}

func (db *DB) Get(key []byte) (value []byte, err error) {

}

func (db *DB) get(key []byte, seq uint64) (value []byte, err error) {

	ikey := makeInternalKey(key, seq, keyTypeVal)

	memDb, memFrozenDb := db.getMems()

	defer func() {
		if memDb != nil {
			memDb.UnRef()
		}

		if memFrozenDb != nil {
			memFrozenDb.UnRef()
		}
	}()

	for _, m := range []*memdb.MemDB{memDb, memFrozenDb} {
		if m == nil {
			continue
		}

		if ok, v, e := memGet(m, ikey, db.s.icmp); ok {
			return append([]byte(nil), v...), e
		}
	}

	v := db.s.version()
	defer v.unRef()

}

func (db *DB) getMems() (memDb *memdb.MemDB, memFrozenDb *memdb.MemDB) {

	db.memMu.Lock()
	defer db.memMu.Unlock()

	if db.memDb != nil {
		db.memDb.Ref()
	}

	if db.frozenMemDb != nil {
		db.frozenMemDb.Ref()
	}

	return db.memDb, db.frozenMemDb
}

func memGet(mdb *memdb.MemDB, ikey internalKey, icmp *iComparer) (ok bool, value []byte, err error) {

	value, err = mdb.FindGE(ikey)

	if err == nil {

		ukey, _, kt, kerr := parseInternalKey(ikey)
		if kerr != nil {
			panic(kerr)
		}

		if icmp.uCompare(ukey, ikey.uKey()) == 0 {
			if kt == keyTypeDel {
				return true, nil, collections.ErrNotFound
			}
			return true, value, nil
		}

	} else if err == collections.ErrNotFound {
		return false, nil, nil
	}

	return false, nil, err

}
