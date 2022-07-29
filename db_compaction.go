package myleveldb

import (
	"errors"
	error2 "myleveldb/error"
	"myleveldb/memdb"
	"myleveldb/storage"
)

type cCmd interface {
	Ack(err error)
}

type cAuto struct {
	err chan error
}

func (c cAuto) Ack(err error) {
	if c.err != nil {
		c.err <- err
	}
}

func (db *DB) mCompaction() {

	var (
		x  cCmd
		ok bool
	)

	defer func() {

		if p := recover(); p != nil {
			x.Ack(errors.New("leveldb/mCompaction panic recovered"))
			x = nil
		}

		if x != nil {
			x.Ack(error2.ErrClosed)
		}

	}()

	for {
		select {
		case cmd := <-db.mcompCmdC:
			x, ok = cmd.(*cAuto)
			if ok {
				x.Ack(db.memCompaction())
			} else {
				x.Ack(errors.New("leveldb/unSupport mem compact cmd"))
			}
			x = nil
		case <-db.closeC:
			return
		}

	}

}

// 内存数据库(frozenMemdb)持久化到level0层
func (db *DB) memCompaction() error {

	frozenMemDb := db.getFrozenMemDb()
	if frozenMemDb == nil {
		return nil
	}

	defer frozenMemDb.UnRef()

	// 如果immutable memdb 是空的, drop掉
	if frozenMemDb.Len() == 0 {
		db.dropFrozenMemDb()
		return nil
	}

	// 在memcompaction期间, 需要暂停其他正在执行的table compaction
	resumeC := make(chan struct{})

	defer func() {
		if resumeC != nil {
			select {
			case <-resumeC:
				close(resumeC)
			}
		}
	}()

	select {
	case db.tPauseCmdC <- resumeC:
	case <-db.closeC:
		close(resumeC)
		resumeC = nil
		db.compactionTransactExit()
	}

	var (
		rec = &SessionRecord{}
	)

	// 将frozenmemdb 写入到sstable中
	if err := db.s.flushMemDb(rec, frozenMemDb); err != nil {
		for _, fd := range rec.atRecords {
			db.s.stor.Remove(storage.FileDesc{Type: storage.FileTypeSSTable, Num: fd.num})
		}
		return err
	}

	// 更新session record, 准备写入到manifest中
	rec.setJournalNum(int64(db.journalFd.Num))
	rec.setSequenceNum(db.frozenSeq)

	// fixme: 可能需要上锁
	if err := db.s.commit(rec); err != nil {
		return err
	}

	// 将frozenmemdb 删掉
	db.dropFrozenMemDb()

	return nil

}

// 将当前memdb 转成frozenMemdb
func (db *DB) rotateMem(n int, wait bool) (*memdb.MemDB, error) {

	// 先让上一次的memCompaction 结束
	err := db.compTriggerWait(db.mcompCmdC)
	if err != nil {
		return nil, err
	}

	newMemDb, err := db.newMem(n)
	if err != nil {
		return nil, err
	}

	if wait {
		_ = db.compTriggerWait(db.mcompCmdC)
	} else {
		_ = db.compTrigger(db.mcompCmdC)
	}
	return newMemDb, nil
}

func (db *DB) getFrozenMemDb() *memdb.MemDB {

	db.memMu.Lock()
	defer db.memMu.Unlock()
	if db.frozenMemDb == nil {
		return nil
	}

	mdb := db.frozenMemDb
	mdb.Ref()
	return mdb
}

func (db *DB) compactionTransactExit() {
	panic(error2.ErrCompactionExit)
}

func (db *DB) dropFrozenMemDb() {
	db.memMu.Lock()
	defer db.memMu.Unlock()
	db.frozenMemDb.UnRef()
	db.frozenMemDb = nil
	db.frozenJournalFd = storage.FileDesc{}
}
