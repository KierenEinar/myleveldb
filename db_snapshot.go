package myleveldb

import "container/list"

type snapshotElement struct {
	seq uint64
	e   *list.Element
	ref uint64
}

func (db *DB) acquireSnapshot() *snapshotElement {
	db.snapMu.Lock()
	defer db.snapMu.Unlock()
	seq := db.loadSeq()

	if e := db.snapList.Back(); e != nil {
		ele := e.Value.(*snapshotElement)
		if ele.seq == seq {
			ele.ref++
		} else if ele.seq > seq {
			panic("myLeveldb/snapshot acquire invalid seq")
		}
		return ele
	}

	se := &snapshotElement{
		seq: seq,
		ref: 1,
	}

	se.e = db.snapList.PushBack(se)

	return se

}

func (db *DB) releaseSnapshot(se *snapshotElement) {

	db.snapMu.Lock()
	defer db.snapMu.Unlock()
	se.ref--
	if se.ref == 0 {
		if db.snapList.Remove(se.e) != nil {
			se.e = nil
		}
		return
	} else if se.ref < 0 {
		panic("myLeveldb/releaseSnapshot invalid ref")
	}
}
