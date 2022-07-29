package error

import (
	"errors"
	"fmt"
	"myleveldb/storage"
)

type ErrCorrupted struct {
	error
	fd   storage.FileDesc
	desc string
}

func NewErrCorrupted(fd storage.FileDesc, desc string) error {
	err := &ErrCorrupted{
		error: fmt.Errorf("ErrCorrupted, fd=%#v, desc=%s", fd, desc),
		fd:    fd,
		desc:  desc,
	}
	return err
}

func IsErrCorruptedErr(err error) bool {
	_, ok := err.(ErrCorrupted)
	return ok
}

type BatchDecodeHeaderErr struct {
	error
	Seq uint64
}

func NewBatchDecodeHeaderErrWithSeq(expectSeq uint64, headerSeq uint64) error {
	err := &BatchDecodeHeaderErr{
		error: fmt.Errorf("batch decode header error, expSeq=%d, headerSeq=%d", expectSeq, headerSeq),
		Seq:   expectSeq,
	}
	return err

}

var (
	ErrClosed         = errors.New("myleveldb/closed")
	ErrHasFrozenMemDb = errors.New("myleveldb/frozen memdb not null")
	ErrCompactionExit = errors.New("myleveldb/compaction transact exit... ")
)
