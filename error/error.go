package error

import (
	"fmt"
	"myleveldb/storage"
)

type ErrCorrupted struct {
	error
	fd   storage.FileDesc
	desc string
}

func NewErrCorrupted(fd storage.FileDesc, desc string) error {
	return fmt.Errorf("err content corrupted, fd is %#v, desc is %s", fd, desc)
}

func IsErrCorruptedErr(err error) bool {
	_, ok := err.(ErrCorrupted)
	return ok
}
