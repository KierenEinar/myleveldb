package myleveldb

import (
	"myleveldb/storage"
	"os"
)

// DB 数据库
type DB struct {
	seq int64 // 时序

	session *Session

	opts *Options
}

// Open 打开数据库
func Open(filepath string) (*DB, error) {

	stor, err := storage.OpenFile(filepath, false)
	if err != nil {
		return nil, err
	}

	session, err := newSession(stor)
	if err != nil {
		return nil, err
	}

	// 恢复manifest的信息到session中
	err = session.recover()
	if err != nil {

		if err != os.ErrNotExist {
			return nil, err
		}

		err = session.create() // 首次打开不存在manifest文件, 创建一个
		if err != nil {
			return nil, err
		}
	}

	return openDB(session)

}
