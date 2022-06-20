package myleveldb

import (
	"bufio"
	"fmt"
	"io"
	"myleveldb/comparer"
	error2 "myleveldb/error"
	"myleveldb/journal"
	"myleveldb/storage"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Session 代表数据库的状态
type Session struct {
	stJournalNum int64  // 当前的日志号码
	nextFileNum  int64  // 下一个可用的文件号码
	seqNum       uint64 // 最近内存compaction的seq

	stor storage.Storage // 存储
	vmu  sync.Mutex

	manifestFd     storage.FileDesc
	manifestWriter storage.Writer
	icmp           comparer.Compare

	// version 相关(mvcc)
	stVersion   *Version // 当前正在使用的版本
	ntVersionId int64    // 下一个版本号码

	// version引用相关
	versionRefCh   chan *VersionRef
	versionDeltaCh chan *VersionDelta
	versionRelCh   chan *VersionRelease
}

// Open 打开存储, 获得session
func Open(stor storage.Storage) (*Session, error) {

	if stor == nil {
		return nil, os.ErrInvalid
	}

	s := &Session{
		stor:           stor,
		versionRefCh:   make(chan *VersionRef),
		versionDeltaCh: make(chan *VersionDelta),
		versionRelCh:   make(chan *VersionRelease),
	}

	go s.refLoop()
	s.setVersion(nil, s.newVersion())
	return s, nil
}

func (s *Session) setVersion(sessionRecord *SessionRecord, newVer *Version) {
	s.vmu.Lock()
	defer s.vmu.Unlock()
	newVer.incRef()

	if s.stVersion != nil {
		if sessionRecord != nil {
			s.stVersion.delta(sessionRecord)
		}
		s.stVersion.unRef()
	}
	s.stVersion = newVer

}

// note 使用前需要上锁
func (v *Version) incRef() {

	if v.released {
		panic(fmt.Errorf("version, id=%d, has been released", v.id))
	}
	v.ref++
	if v.ref == 1 {
		v.session.versionRefCh <- &VersionRef{
			vid:        v.id,
			files:      v.levels,
			createTime: time.Now(),
		}
	}

}

// note 使用前需要上锁
func (v *Version) unRef() {

	if v.released {
		panic(fmt.Errorf("version, id=%d, has been released", v.id))
	}

	v.ref--

	if v.ref < 0 {
		panic(fmt.Errorf("version, id=%d, ref negative ", v.id))
	}

	if v.ref == 0 {
		v.session.versionRelCh <- &VersionRelease{
			vid:   v.id,
			files: v.levels,
		}
	}

}

func (v *Version) delta(sessionRecord *SessionRecord) {

	added := make([]int64, 0, len(sessionRecord.atRecords))
	del := make([]int64, 0, len(sessionRecord.dlRecords))

	for _, v := range sessionRecord.atRecords {
		added = append(added, int64(v.num))
	}

	for _, v := range sessionRecord.dlRecords {
		del = append(del, int64(v.num))
	}

	v.session.versionDeltaCh <- &VersionDelta{
		vid:     v.id,
		added:   added,
		deleted: del,
	}
}

func (s *Session) newVersion() *Version {

	s.vmu.Lock()
	defer s.vmu.Unlock()
	versionId := s.ntVersionId
	s.ntVersionId = versionId + 1
	return &Version{
		id:      versionId,
		session: s,
	}
}

func (s *Session) recover() (err error) {

	defer func() {
		if err == os.ErrNotExist {
			err = nil
		}
		return
	}()

	fd, err := s.stor.GetMeta()
	if err != nil {
		return err
	}

	reader, err := s.stor.Open(fd)
	if err != nil {
		return err
	}

	var (
		sessionRecord  = &SessionRecord{}
		journalReader  = journal.NewReader(reader)
		versionStaging = s.stVersion.newVersionStaging()
	)

	for {

		chunkReader, err := journalReader.SeekNextChunk()
		if err == io.EOF {
			break
		}

		err = sessionRecord.decode(bufio.NewReader(chunkReader))
		if err != nil {
			return
		}

		versionStaging.commit(sessionRecord)

		sessionRecord.resetAddRecord()
		sessionRecord.resetDelRecord()
		sessionRecord.resetCompatPtr()

	}

	switch {
	case !sessionRecord.hasField(recComparer):
		return error2.NewErrCorrupted(fd, "manifest lack recComparer")
	case !sessionRecord.hasField(recSequenceNum):
		return error2.NewErrCorrupted(fd, "manifest lack recSequenceNum")
	case !sessionRecord.hasField(recNextFileNum):
		return error2.NewErrCorrupted(fd, "manifest lack recNextFileNum")
	case !sessionRecord.hasField(recJournalNum):
		return error2.NewErrCorrupted(fd, "manifest lack recJournalNum")
	}

	s.setVersion(sessionRecord, versionStaging.finish())
	s.SetNextFileNum(sessionRecord.nextFileNum)
	s.commitRecord(sessionRecord)
	return
}

func (s *Session) SetNextFileNum(nextFileNum int64) {
	atomic.StoreInt64(&s.nextFileNum, nextFileNum)
}

func (s *Session) commitRecord(sessionRecord *SessionRecord) {

	if sessionRecord.hasField(recSequenceNum) {
		s.seqNum = recSequenceNum
	}

	if sessionRecord.hasField(recJournalNum) {
		s.stJournalNum = recJournalNum
	}

	// todo 将compatkey记录到Session

}
