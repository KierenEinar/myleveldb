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
	c.err <- err
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

func (db *DB) tableAutoCompaction() {
	if c := db.s.pickCompaction(); c != nil {
		db.tableCompaction(c)
	}
}

func (db *DB) tableCompaction(c *Compaction) error {

	var (
		lastUKey     []byte
		hashLastUKey bool
		lastSeq      uint64
		tw           *tWriter
		sr           SessionRecord
		dropped      bool
	)

	// 如果input跟合并层不存在重复的话(意味着level[1]=nil), 并且跟合并层的下一层(gp)不超过默认(10)个文件的时候,
	// 那么可以直接将input层放到compaction层
	if c.trivial() {
		for _, t0 := range c.levels[0] {
			sr.addTableFile(c.sourceLevel+1, t0)
			sr.delTableFile(c.sourceLevel, t0)
		}
		if err := c.s.commit(&sr); err != nil {
			return err
		}
		return nil
	}

	for i, tables := range c.levels {
		for _, table := range tables {
			sr.delTableFile(c.sourceLevel+i, table) // 对需要合并的文件全部置为删除
		}
	}

	iter := c.newIterator(db.s.tableOpts)
	defer iter.UnRef()

	seq := db.minSeq() // 获取db当前正在被使用的最小seq(如果存在快照, 那么取快照头部(最老旧)的seq, 不存在则取当前seq)

	for iter.Next() {
		iKey := iter.Key()

		ukey, uSeq, kType, err := parseInternalKey(iKey)

		if err == nil {

			/*
				需要判断如果加上当前ukey, 跟gp的重叠过多, 那么需要暂停之前的合并, 并把之前的合并直接写到.ldb文件, 再开一个新的.ldb文件
			*/
			if !hashLastUKey || db.s.icmp.uCompare(ukey, lastUKey) != 0 { // ukey首次合并写入
				shouldStop := c.shouldStopBefore(ukey)

				if tw == nil {
					if tw, err = db.s.tableOpts.create(0); err != nil {
						return err
					}
				}

				// 如果要写入的文件跟sstable
				if shouldStop || tw.tableWriter.BytesLen() >= uint64(db.s.GetTableFileSize()) {
					tf, err := tw.finish()
					if err != nil {
						return err
					}
					sr.addTableFile(c.sourceLevel+1, *tf)
					tw = nil
					c.restore()
				}
				hashLastUKey = true
				lastUKey = ukey
				lastSeq = maxSeq
			}

			/**
				重新调整的规则

				/-------------------------------------------------------------------------/
																	|
			 													  minseq
				1. 如果当前ukey是首次compact
					*. 如果seq>=minseq, 那么直接保存到.ldb文件
				    *. 否则, 普通设置的话直接保存到.ldb文件, 删除见第三点说明
			    2. 如果当前ukey非首次compact
					*. 如果seq>=minseq, 那么直接保存到.ldb文件
					*. 否则, 直接丢弃
			    3. 如果当前level和level+1存在seq小于等于minseq并且是删除行为, 需要判断level+2直到最高层存不存在该key,
				   如果存在的话则不能删除, 否则会造成本来已经删除的key, 反而能被搜索到

			**/
			if lastSeq <= seq {
				dropped = true
			} else if kType == keyTypeDel && uSeq <= seq && c.isBaseLevelForKey(ukey) {
				dropped = true
			}
			lastSeq = seq

			if !dropped {
				if tw == nil {
					if tw, err = db.s.tableOpts.create(0); err != nil {
						return err
					}
				}
				tw.append(iKey, iter.Value())
			}
		}

	}

	if tw != nil {
		if tf, err := tw.finish(); err != nil {
			return err
		} else {
			sr.addTableFile(c.sourceLevel+1, *tf)
		}
	}

	return db.s.commit(&sr)
}
