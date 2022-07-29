package myleveldb

import (
	"io"
	"io/ioutil"
	error2 "myleveldb/error"
	"myleveldb/journal"
	"myleveldb/memdb"
	"myleveldb/storage"
	"sort"
)

/**
wal日志恢复

分为几个步骤
1. 先从stor获取所有的journal文件
2. 对大于等于stJournalNum的进行一次replay操作
	replay操作如下:
		遍历所有的筛选后的日志文件
			对当前正在遍历的日志文件进行迭代, 先写入到memtable, 当memtable满了之后, 落地到sstable, 持久化到manifest文件中
		如果当前文件日志文件的内容仍旧存在于memtable中, 那么还是需要落地到sstable, 持久化到manifest文件中


**/
func (db *DB) recoverJournal() error {

	fds, err := db.s.stor.List(storage.FileTypeJournal)
	if err != nil {
		return err
	}

	sortFds(fds)

	var (
		ofd storage.FileDesc
		rec = &SessionRecord{}
		mdb = memdb.NewMemDB(db.s.Options.GetWriteBuffer(), db.s.icmp, db.pool)
	)

	if len(fds) > 0 {

		db.s.markFileNum(int64(fds[len(fds)-1].Num))

		var (
			jr          *journal.Reader
			reader      storage.Reader
			chunkReader io.Reader
		)

		for _, fd := range fds {
			reader, err = db.s.stor.Open(fd)
			if err != nil {
				return err
			}

			jr = journal.NewReader(reader)

			// 如果上个journal遍历存在, 那么需要把它更新到manifest中
			if !ofd.Zero() {
				if mdb.Len() > 0 {
					err = db.s.flushMemDb(rec, mdb)
					if err != nil {
						return err
					}
				}
				rec.setJournalNum(int64(fd.Num))
				rec.setSequenceNum(db.seq)

				err = db.s.commit(rec) // 更新到manifest中, 并更新到当前session和version中
				if err != nil {
					return err
				}

				// 把journal文件删除
				db.s.stor.Remove(ofd)

				ofd = storage.FileDesc{}

				rec.resetAddRecord()
			}

			for {

				chunkReader, err = jr.SeekNextChunk()
				if err == io.EOF {
					break
				}

				if err != nil {
					return err
				}

				chunk, err := ioutil.ReadAll(chunkReader)
				if err != nil {
					return err
				}

				batchSeq, batchLen, err := decodeBatchToMem(chunk, db.seq, mdb)
				if err != nil {
					return err
				}

				db.seq = batchSeq + uint64(batchLen)

				if mdb.Len() >= db.s.Options.GetWriteBuffer() {
					// 将内存数据库dump到level0的sstable file中
					err = db.s.flushMemDb(rec, mdb)
					if err != nil {
						return err
					}
					mdb.Reset()
				}

			}

			ofd = fd

			reader.Close()

		}

		// 对最后一个journal进行刷新到mdb, 再更新到manifest中
		if mdb.Len() > 0 {
			err = db.s.flushMemDb(rec, mdb)
			if err != nil {
				return err
			}
		}

	}

	// 创建一个新的memdb和journal
	mdb, err = db.newMem(0)
	if err != nil {
		return err
	}
	defer mdb.UnRef()
	rec.setSequenceNum(db.seq)
	rec.setJournalNum(int64(db.journalFd.Num))

	err = db.s.commit(rec)
	if err != nil {
		return err
	}

	if !ofd.Zero() {
		db.s.stor.Remove(ofd)
	}

	return nil
}

func sortFds(fds []storage.FileDesc) {
	sort.Slice(fds, func(i, j int) bool {
		return fds[i].Num < fds[j].Num
	})
}

func (db *DB) newMem(size int) (*memdb.MemDB, error) {

	db.memMu.Lock()
	defer db.memMu.Unlock()

	if db.frozenMemDb != nil {
		return nil, error2.ErrHasFrozenMemDb
	}

	journalFd := storage.FileDesc{Type: storage.FileTypeJournal, Num: int(db.s.allocNextNum())}
	writer, err := db.s.stor.Create(journalFd)
	if err != nil {
		return nil, err
	}

	journalWriter := journal.NewWriter(writer)

	if db.journal != nil {
		db.journalWriter.Close()
	}

	memDb := memdb.NewMemDB(size, db.s.icmp, db.pool)

	db.frozenMemDb = db.memDb
	db.frozenJournalFd = db.journalFd

	memDb.Ref() // 自己
	memDb.Ref() // 调用方
	db.memDb = memDb

	db.journal = journalWriter
	db.journalFd = journalFd
	db.journalWriter = writer

	db.frozenSeq = db.seq

	return memDb, nil

}
