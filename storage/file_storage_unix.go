// +build darwin dragonfly freebsd linux netbsd openbsd

package storage

import (
	"os"
	"syscall"
)

type UnixFileLock struct {
	*os.File
	readOnly bool
}

func newFileLock(path string, readOnly bool) (*UnixFileLock, error) {

	var flag int

	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_RDWR
	}

	f, err := os.OpenFile(path, flag|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	err = setFileLock(f, true, readOnly)
	if err != nil {
		return nil, err
	}

	return &UnixFileLock{
		File:     f,
		readOnly: readOnly,
	}, nil

}

func setFileLock(f *os.File, lock, readOnly bool) error {
	how := syscall.LOCK_UN
	if lock {
		if readOnly {
			how = syscall.LOCK_SH
		} else {
			how = syscall.LOCK_EX
		}
	}
	return syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB)
}

func (fl *UnixFileLock) Release() error {
	return setFileLock(fl.File, false, fl.readOnly)
}

func syncDir(name string) error {
	f, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
