package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

var (
	ErrFileDesc   = errors.New("file desc not ok")
	ErrFileDir    = errors.New("file is dir err")
	ErrFileClosed = errors.New("file is closed")
	ErrStorClosed = errors.New("storage is closed")
	ErrReadOnly   = errors.New("read only err")
	ErrShortWrite = errors.New("write short err")
	ErrCorupted   = errors.New("corupted content err")
)

// FileStorage 文件存储
type FileStorage struct {
	dir      string   // 文件目录
	flock    FileLock // 系统文件锁
	readOnly bool
	open     int
	mutex    sync.RWMutex
}

// OpenFile 打开存储, path必须是目录的路径
// 需要依赖于操作系统的文件锁
func OpenFile(path string, readOnly bool) (Storage, error) {
	if fd, err := os.Stat(path); err == nil {
		if !fd.IsDir() {
			return nil, ErrFileDesc
		}
	} else if os.IsNotExist(err) && !readOnly {
		if err = os.MkdirAll(path, 0644); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	flock, err := newFileLock(filepath.Join(path, "LOCK"), readOnly)
	if err != nil {
		return nil, err
	}

	fs := &FileStorage{
		dir:      path,
		readOnly: readOnly,
		flock:    flock,
	}

	runtime.KeepAlive(fs)
	runtime.SetFinalizer(fs, (*FileStorage).Close)

	return fs, nil

}

type fileReader struct {
	*os.File
	fs *FileStorage
}

func (fr *fileReader) Close() (err error) {

	fs := fr.fs
	fs.mutex.Lock()
	defer func() {
		if err == nil {
			fs.open--
		}
		fs.mutex.Unlock()
	}()

	if fs.open < 0 {
		return ErrStorClosed
	}

	err = fr.File.Close()
	return
}

// Open 打开文件
func (fs *FileStorage) Open(fd FileDesc) (Reader, error) {
	if !fd.FileDescOK() {
		return nil, ErrFileDesc
	}

	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	if fs.open < 0 {
		return nil, ErrStorClosed
	}

	name := fsGenFileName(fd)

	f, err := os.OpenFile(filepath.Join(fs.dir, name), os.O_RDONLY, 0644)
	if err == nil {
		if fs, err := f.Stat(); err != nil {
			return nil, err
		} else {
			if fs.IsDir() {
				return nil, ErrFileDir
			}
		}
	} else {
		return nil, err
	}

	fs.open++

	return &fileReader{f, fs}, nil
}

// List list ft 的所有fd
func (fs *FileStorage) List(ft FileType) ([]FileDesc, error) {

	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	if fs.open < 0 {
		return nil, ErrStorClosed
	}

	f, err := os.OpenFile(filepath.Join(fs.dir), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	names, err := f.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	fds := make([]FileDesc, 0)

	for _, name := range names {
		var fd FileDesc
		if ok := fsParseName(name, &fd); ok && fd.Type == ft {
			fds = append(fds, fd)
		}
	}
	return fds, nil
}

// Create 创建一个文件
func (fs *FileStorage) Create(fd FileDesc) (Writer, error) {

	if !fd.FileDescOK() {
		return nil, ErrFileDesc
	}

	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if fs.open < 0 {
		return nil, ErrFileClosed
	}

	if fs.readOnly {
		return nil, ErrReadOnly
	}

	path := filepath.Join(fs.dir, fsGenFileName(fd))

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		return nil, err
	}

	if fstat, err := f.Stat(); err != nil {
		return nil, err
	} else {
		if fstat.IsDir() {
			return nil, ErrFileDir
		}
	}

	fs.open++

	return &fileWriter{f, fs, fd}, nil

}

func (fs *FileStorage) Remove(fd FileDesc) error {

	if !fd.FileDescOK() {
		return ErrFileDesc
	}

	if fs.readOnly {
		return ErrReadOnly
	}

	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if fs.open < 0 {
		return ErrStorClosed
	}

	return os.Remove(filepath.Join(fs.dir, fsGenFileName(fd)))
}

func (fs *FileStorage) Rename(oldFd, newFd FileDesc) error {

	if !oldFd.FileDescOK() || !newFd.FileDescOK() {
		return ErrFileDesc
	}

	if fs.readOnly {
		return ErrReadOnly
	}

	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if fs.open < 0 {
		return ErrStorClosed
	}

	return os.Rename(filepath.Join(fs.dir, fsGenFileName(oldFd)),
		filepath.Join(fs.dir, fsGenFileName(newFd)))
}

// SetMeta 设置元信息保存在哪个文件
func (fs *FileStorage) SetMeta(fd FileDesc) error {

	if !fd.FileDescOK() {
		return ErrFileDesc
	}

	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if fs.open < 0 {
		return ErrFileClosed
	}

	if fs.readOnly {
		return ErrReadOnly
	}

	return fs.setMeta(fd)

}

func (fs *FileStorage) setMeta(fd FileDesc) error {

	const currentPath = "CURRENT"
	const backupPath = currentPath + ".bak"
	currentTempPath := fmt.Sprintf("%s.%06d", currentPath, fd.Num)
	const perm = 0644
	newContent := fsGenFileName(fd) + "\n"

	fullCurrentPath := filepath.Join(fs.dir, currentPath)

	cf, err := os.OpenFile(fullCurrentPath, os.O_RDONLY, perm)
	if os.IsNotExist(err) { // 说明当前要写入的文件不存在
		return writeFileSynced(fullCurrentPath, []byte(newContent), perm)
	}

	if err != nil {
		return err
	}

	defer cf.Close()

	// current 文件存在
	oldContent, err := ioutil.ReadAll(cf)
	if err != nil {
		return err
	}

	// 当前内容一致, 不需要再次写入, 直接返回
	if string(oldContent) == newContent {
		return nil
	}

	// 写入备份
	fullBackupCurrentPath := filepath.Join(fs.dir, backupPath)
	if err = writeFileSynced(fullBackupCurrentPath, oldContent, perm); err != nil {
		return err
	}

	// 写入current.%06d
	fullCurrentTmpPath := filepath.Join(fs.dir, currentTempPath)
	if err = writeFileSynced(fullCurrentTmpPath, []byte(newContent), perm); err != nil {
		return err
	}

	// rename 名字
	if err = os.Rename(fullCurrentPath, fullCurrentTmpPath); err != nil {
		return err
	}

	// sync root
	return syncDir(fs.dir)

}

// GetMeta 获取meta信息指向的fd
func (fs *FileStorage) GetMeta() (FileDesc, error) {

	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if fs.open < 0 {
		return FileDesc{}, ErrFileClosed
	}

	// 获取current, current.bak, current.%06d所有文件, 然后按内容排序, num最大的就返回并设置为current

	type CurrentFile struct {
		fd   FileDesc
		name string
	}

	tryCurrent := func(name string) (*CurrentFile, error) {

		var fd FileDesc

		f, err := os.OpenFile(filepath.Join(fs.dir, name), os.O_RDONLY, 0644)
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		} else if err != nil {
			return nil, err
		}

		defer f.Close()

		contents, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, err
		}

		if len(contents) > 1 && contents[len(contents)-1] == '\n' &&
			fsParseName(string(contents[:len(contents)-1]), &fd) {

			if _, err := os.Stat(filepath.Join(fs.dir, fsGenFileName(fd))); err == nil {
				return &CurrentFile{
					fd:   fd,
					name: name,
				}, nil
			} else if os.IsNotExist(err) {
				return nil, os.ErrNotExist
			}
		}

		return nil, ErrCorupted
	}

	tryCurrents := func(names []string) (*CurrentFile, error) {

		var err1 error

		for _, name := range names {
			fd, err := tryCurrent(name)
			if err == nil {
				return fd, nil
			} else if err == ErrCorupted {
				err1 = ErrCorupted
				continue
			} else if err == os.ErrNotExist {
				err1 = os.ErrNotExist
				continue
			} else {
				return nil, err
			}
		}

		return nil, err1
	}

	dir, err := os.OpenFile(fs.dir, os.O_RDONLY, 0644)
	if err != nil {
		return FileDesc{}, err
	}

	defer dir.Close()

	names, err := dir.Readdirnames(0)
	if err != nil {
		return FileDesc{}, err
	}

	tempCurrents := make([]string, 0)

	for _, name := range names {
		if strings.HasPrefix(name, "CURRENT.") && name != "CURRENT.bak" {
			var num int
			_, err = fmt.Sscanf(name, "CURRENT.%06d", &num)
			if err == nil {
				tempCurrents = append(tempCurrents, name)
			}
		}
	}

	sort.Slice(tempCurrents, func(i, j int) bool {

		var (
			iNum int
			jNum int
		)

		_, _ = fmt.Sscanf(tempCurrents[i], "CURRENT.%06d", &iNum)
		_, _ = fmt.Sscanf(tempCurrents[j], "CURRENT.%06d", &jNum)
		return iNum > jNum
	})

	var (
		pendingCur *CurrentFile
		cur        *CurrentFile
	)

	if len(tempCurrents) > 0 {
		pendingCur, err = tryCurrents(tempCurrents)
		if err != nil && err != os.ErrNotExist && err != ErrCorupted {
			return FileDesc{}, err
		}
	}

	cur, err = tryCurrents([]string{"CURRENT", "CURRENT.bak"})
	if err != nil && err != os.ErrNotExist && err != ErrCorupted {
		return FileDesc{}, err
	}

	if pendingCur != nil && (cur == nil || pendingCur.fd.Num > cur.fd.Num) {
		cur = pendingCur
	}

	if cur != nil {

		if !fs.readOnly && (cur.name != "CURRENT" || len(tempCurrents) > 0) {
			if err := fs.setMeta(cur.fd); err == nil {
				for _, v := range tempCurrents {
					os.Remove(filepath.Join(fs.dir, v))
				}
			}
		}

	}

	if cur == nil {
		return FileDesc{}, err
	}

	return cur.fd, nil

}

//func (fs *FileStorage) Lock() (Locker, error) {
//
//}
//
func (fs *FileStorage) Close() error {

	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if fs.open < 0 {
		return ErrStorClosed
	}

	if fs.open > 0 {
		fmt.Println("open > 0")
	}

	runtime.SetFinalizer(fs, nil)

	return fs.flock.Release()

}

type fileWriter struct {
	*os.File
	fileStorage *FileStorage
	fd          FileDesc
}

func (fw *fileWriter) Sync() error {
	if err := fw.File.Sync(); err != nil {
		return err
	}

	if fw.fd.Type == FileTypeManifest {
		return syncDir(fw.fileStorage.dir)
	}

	return nil
}

func (fw *fileWriter) Close() (err error) {
	fw.fileStorage.mutex.Lock()
	defer func() {
		if err == nil {
			fw.fileStorage.open--
		}
		fw.fileStorage.mutex.Unlock()
	}()

	err = fw.File.Close()
	return

}

func fsParseName(fdName string, fd *FileDesc) bool {
	var tail string
	_, err := fmt.Sscanf(fdName, "%06d.%s", fd.Num, &tail)
	if err == nil {

		switch tail {
		case "log":
			fd.Type = FileTypeJournal
		case "ldb":
			fd.Type = FileTypeSSTable
		case "temp":
			fd.Type = FileTypeTemp
		default:
			return false
		}

		return true
	}

	n, _ := fmt.Sscanf(fdName, "MANIFEST-%06d", &fd.Num)
	if n == 1 {
		fd.Type = FileTypeManifest
		return true
	}

	return false
}

func writeFileSynced(path string, content []byte, perm os.FileMode) (err error) {

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	defer file.Close()

	n, err := file.Write(content)
	if err != nil {
		return err
	}

	if n < len(content) {
		return ErrShortWrite
	}

	if err = file.Sync(); err != nil {
		return err
	}

	return
}
