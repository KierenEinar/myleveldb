package myleveldb

import (
	"bytes"
	error2 "myleveldb/error"
	"myleveldb/memdb"
	"sync"
)

var (
	writeBatchPool = sync.Pool{
		New: func() interface{} {
			return &Batch{
				data: bytes.NewBuffer(nil),
			}
		},
	}
)

type WithBatch struct {
	makeRoomForWrite func(n int) (*memdb.MemDB, int, error)
	writeBatch       func(b *Batch, memDb *memdb.MemDB, mdbFree int) error
}

func getWriteBatch() *Batch {
	return writeBatchPool.Get().(*Batch)
}

func putWriteBatch(batch *Batch) {
	batch.reset()
	writeBatchPool.Put(batch)
}

// WriteMerge 合并写
type WriteMerge struct {
	writeMergeC  chan writeMerge
	writeMergedC chan bool
	writeLock    chan struct{}
	writeAck     chan error
	closedC      chan struct{}
}

func NewWriteMerge() *WriteMerge {
	wm := &WriteMerge{
		writeMergeC:  make(chan writeMerge),
		writeMergedC: make(chan bool),
		writeLock:    make(chan struct{}, 1),
		writeAck:     make(chan error),
		closedC:      make(chan struct{}),
	}
	return wm
}

type writeMerge struct {
	kt         keyType
	key, value []byte
}

// Put 写入单条记录, 支持并发合并写
func (wb *WriteMerge) Put(kt keyType, key, value []byte, withBatch *WithBatch) error {

	select {

	case wb.writeMergeC <- writeMerge{kt, key, value}:
		if <-wb.writeMergedC {
			return <-wb.writeAck
		}

	case <-wb.closedC:
		return error2.ErrClosed

	case wb.writeLock <- struct{}{}: // 拿到写锁

	}

	batch := getWriteBatch()
	batch.appendEntry(kt, key, value)
	return wb.writeLocked(batch, withBatch)
}

func (wm *WriteMerge) writeLocked(batch *Batch, withBatch *WithBatch) error {

	if withBatch == nil {
		panic("withBatch cb 不能为空")
	}

	mdb, mdbFree, err := withBatch.makeRoomForWrite(batch.internalLen)
	if err != nil {
		return err
	}
	defer mdb.UnRef()

	var (
		mergeLimit int
		overflow   bool
		merged     int
		mergeCap   int
	)

	if batch.internalLen > 128<<10 {
		mergeLimit = 1024<<10 - batch.internalLen // 1m
	} else {
		mergeLimit = 128<<10 - batch.internalLen //  128k
	}

	mergeCap = mdbFree - batch.internalLen
	if mergeLimit > mergeCap {
		mergeLimit = mergeCap
	}

merge:
	for {

		select {
		case incoming := <-wm.writeMergeC:

			k, v, kt := incoming.key, incoming.value, incoming.kt
			iLen := len(k) + len(v) + 8

			mergeLimit -= iLen
			if mergeLimit < 0 {
				overflow = true
				break merge
			}

			batch.appendEntry(kt, k, v)

			wm.writeMergedC <- true
			merged++
		default:
			break merge
		}

	}

	defer putWriteBatch(batch)

	if err := withBatch.writeBatch(batch, mdb, mdbFree); err != nil {
		return wm.unLockWrite(overflow, merged, err)
	}

	return wm.unLockWrite(overflow, merged, nil)
}

func (wm *WriteMerge) unLockWrite(overflow bool, merged int, err error) error {

	for i := 0; i < merged; i++ {
		wm.writeAck <- err
	}

	if overflow {
		wm.writeMergedC <- false
	} else {
		<-wm.writeLock
	}

	return err
}
