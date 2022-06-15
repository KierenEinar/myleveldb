package myleveldb

import (
	"myleveldb/comparer"
	"myleveldb/storage"
	"sync"
)

// Session 代表数据库的状态
type Session struct {
	stJournalNum int64  // 当前的日志号码
	nextFileNum  int64  // 下一个可用的文件号码
	seqNum       uint64 // 最近内存compaction的seq

	stVersion   *Version // 当前正在使用的版本
	ntVersionId int64    // 下一个版本号码

	stor storage.Storage // 存储
	vmu  sync.Mutex

	manifestFd storage.FileDesc

	icmp comparer.Compare
}

// Open 打开存储, 获得session
func Open(stor storage.Storage) *Session {

}

func (s *Session) recover() error {

}
