package storage

import (
	"fmt"
	"io"
)

// FileType 文件类型
type FileType int

const (
	FileTypeManifest FileType = 1 << iota
	FileTypeJournal
	FileTypeSSTable
	FileTypeTemp
	FileAll = FileTypeManifest | FileTypeJournal | FileTypeSSTable | FileTypeTemp
)

// FileDesc stor描述符
type FileDesc struct {
	Type FileType
	Num  int
}

func fsGenFileName(fd FileDesc) string {

	switch fd.Type {
	case FileTypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case FileTypeJournal:
		return fmt.Sprintf("%06d.log", fd.Num)
	case FileTypeSSTable:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	case FileTypeTemp:
		return fmt.Sprintf("%06d.temp", fd.Num)
	default:
		return fmt.Sprintf("%06d.%x", fd.Num, fd.Type)
	}

}

func (fd *FileDesc) FileDescOK() bool {
	switch fd.Type {
	case FileTypeManifest, FileTypeJournal, FileTypeSSTable, FileTypeTemp:
	default:
		return false
	}
	return fd.Num > 0
}

// Syncer 文件刷盘
type Syncer interface {
	Sync() error
}

// Reader 输入流
type Reader interface {
	io.ReadSeeker
	io.ReaderAt
	io.Closer
}

// FileLock 系统文件lock
type FileLock interface {
	Release() error
}

// Writer 写入流
type Writer interface {
	io.WriterAt
	io.Writer
	Syncer
	io.Closer
}

// Locker locker
type Locker interface {
	UnLock() error
}

// Storage 存储层导出接口
type Storage interface {
	// Lock 上锁
	//Lock() (Locker, error)

	// List 根据filetype 列出所有的fd
	List(ft FileType) ([]FileDesc, error)

	// Open 打开文件, 必须是文件
	Open(fd FileDesc) (Reader, error)

	// GetMeta 获取存储stor的meta文件
	GetMeta() (FileDesc, error)

	// SetMeta 设置存储stor的meta文件
	SetMeta(fd FileDesc) error

	// Remove 删除文件
	Remove(fd FileDesc) error

	// Rename 重命名
	Rename(oldFd, newFd FileDesc) error

	// Create 创建文件, 如果文件存在, 内容会被truncate
	Create(fd FileDesc) (Writer, error)

	io.Closer
}
