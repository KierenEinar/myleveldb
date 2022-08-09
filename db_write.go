package myleveldb

import (
	error2 "myleveldb/error"
	"myleveldb/memdb"
	"time"
)

/**
写入时, 需要有条件的控制下写入速度

1. 当发现level0文件个数大于等于 slowdownTrigger时(通常是8个), 那么首次休眠1毫秒
2. 当发现memdb足够写入的长度时立即返回
3. 当发现level0文件个数大于等于 pauseTrigger时(通常是12个), 发起table compaction
4. 当发现memdb不够长度时, 那么将当前memdb转化为frozenMemdb, 如果存在了frozenMemdb, 先执行minorCompaction

**/

const ()

// n 是要写入的长度
func (db *DB) makeRoomForWrite(n int) (memDb *memdb.MemDB, mdbFree int, err error) {

	delay := false

	flush := func() (retry bool) {

		memDb, err = db.getEffectiveMemDb()
		if err != nil {
			return false
		}

		defer func() {
			if retry {
				memDb.UnRef()
			}
		}()

		mdbFree, err := memDb.Free()
		if err != nil {
			return false
		}

		if db.s.tLen(0) >= defaultSlowDownTrigger && !delay {
			delay = true
			time.Sleep(time.Millisecond)
		} else if mdbFree >= n {
			return false
		} else if db.s.tLen(0) >= defaultPauseTrigger {
			delay = true
			err = db.compTriggerWait(db.tcompCmdC)
			if err != nil {
				mdbFree = 0
				return false
			}
		} else {

			// 说明memdb free不够写入的n
			if memDb.Len() == 0 { // 还没有数据
				mdbFree = n
				return false
			}
			memDb.UnRef()
			memDb, err = db.rotateMem(n, false) // 将当前memdb 转成frozenMemdb
			if err != nil {
				mdbFree = 0
				return false
			}
			mdbFree = memDb.Len()
			return false

		}

		return true

	}

	for flush() {
	}

	return
}

func (db *DB) getEffectiveMemDb() (*memdb.MemDB, error) {

	db.memMu.Lock()
	defer db.memMu.Unlock()
	if db.memDb == nil {
		return nil, error2.ErrClosed
	}
	db.memDb.Ref()
	return db.memDb, nil
}

func (db *DB) compTriggerWait(cmd chan<- cCmd) error {
	c := make(chan error)
	defer close(c)
	select {
	case cmd <- cAuto{c}:
	case <-db.closeC:
		return error2.ErrClosed
	}

	select {
	case e := <-c:
		return e
	case <-db.closeC:
		return error2.ErrClosed
	}
}

func (db *DB) compTrigger(cmd chan<- cCmd) error {

	select {
	case cmd <- cAuto{}:
	case <-db.closeC:
		return error2.ErrClosed
	}
	return nil
}

func (db *DB) Put(key, value []byte) error {
	return db.putRec(key, value, keyTypeVal)
}

func (db *DB) putRec(key, value []byte, kt keyType) error {
	return db.writeMerge.Put(kt, key, value, db.withBatch)
}

func (db *DB) writeBatchLocked(b *Batch, mdb *memdb.MemDB, mdbFree int) error {

	seq := db.seq + 1

	// 写入到journal 中
	if err := writeBatchWithHeader(db.journal, seq, b); err != nil {
		return err
	}

	for idx, batchIndex := range b.index {
		ik := makeInternalKey(batchIndex.key(b.data.Bytes()), seq+uint64(idx), batchIndex.KeyType)
		err := mdb.Put(ik, batchIndex.value(b.data.Bytes()))
		if err != nil {
			return err
		}
	}

	db.addSeq(uint64(b.BatchLen()))

	if b.internalLen >= mdbFree {
		db.rotateMem(0, false)
	}

	return nil

}
