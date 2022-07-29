package myleveldb

import (
	"bufio"
	"fmt"
	"io"
	"myleveldb/comparer"
	error2 "myleveldb/error"
	"myleveldb/journal"
	"myleveldb/memdb"
	"myleveldb/storage"
	"myleveldb/utils"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultManifestNamespace = "manifest"
)

// Session 代表数据库的状态
type Session struct {
	// manifest 文件相关
	stJournalNum  int64  // 当前的日志号码, 对应于当前的memtable
	stSeqNum      uint64 // 最近内存compaction的seq
	stNextFileNum int64  // 下一个可用的文件号码

	stor storage.Storage // 存储
	vmu  sync.Mutex

	icmp comparer.BasicComparer

	// version 相关(mvcc)
	stVersion   *Version // 当前正在使用的版本
	ntVersionId int64    // 下一个版本号码

	// version引用相关
	versionRefCh   chan *VersionRef
	versionDeltaCh chan *VersionDelta
	versionRelCh   chan *VersionRelease

	// manifest相关
	manifestFd     storage.FileDesc
	manifestWriter storage.Writer
	manifest       *journal.Writer

	// journal相关
	journalWriter *journal.Writer

	// sstable 操作相关
	tableOpts *sstableOperation
	iFilter   iFilter

	// 选项
	*Options
}

// 打开存储, 获得session
func newSession(stor storage.Storage, opt *Options) (*Session, error) {

	if stor == nil {
		return nil, os.ErrInvalid
	}

	s := &Session{
		stor:           stor,
		versionRefCh:   make(chan *VersionRef),
		versionDeltaCh: make(chan *VersionDelta),
		versionRelCh:   make(chan *VersionRelease),
	}

	s.dupOptions(opt)
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
	atomic.StoreInt64(&s.stNextFileNum, nextFileNum)
}

func (s *Session) commitRecord(sessionRecord *SessionRecord) {

	if sessionRecord.hasField(recSequenceNum) {
		s.stSeqNum = recSequenceNum
	}

	if sessionRecord.hasField(recJournalNum) {
		s.stJournalNum = recJournalNum
	}

	// todo 将compatkey记录到Session

}

// 创建manifest, 并持久化到文件系统(用于首次初始化db)
func (s *Session) create() error {

	fd := storage.FileDesc{Type: storage.FileTypeManifest, Num: int(s.allocNextNum())}

	writer, err := s.stor.Create(fd)
	if err != nil {
		return err
	}

	v := s.version()
	sr := &SessionRecord{}

	s.fillRecord(sr, true)
	v.fillRecord(sr)

	jw := journal.NewWriter(writer)

	writerBuffer := utils.GetPoolNamespace(defaultManifestNamespace)
	defer func() {
		utils.PutPoolNamespace(defaultManifestNamespace, writerBuffer)
	}()

	err = sr.encode(writerBuffer)
	if err != nil {
		return err
	}

	_, err = jw.Write(writerBuffer.Bytes())
	if err != nil {
		return err
	}

	s.manifestFd = fd
	s.manifestWriter = writer
	s.manifest = jw
	return nil
}

func (s *Session) loadNextNum() int64 {
	return atomic.LoadInt64(&s.stNextFileNum)
}

func (s *Session) allocNextNum() int64 {
	return atomic.AddInt64(&s.stNextFileNum, 1)
}

func (s *Session) version() *Version {
	s.vmu.Lock()
	defer s.vmu.Unlock()
	s.stVersion.incRef()
	return s.stVersion
}

// 填充sessionRecord, 如果snapShot为true, 会有条件的填充sessionRecord的其他字段
func (s *Session) fillRecord(sr *SessionRecord, snapShot bool) {
	sr.setNextFileNum(s.loadNextNum())
	if snapShot {
		if !sr.hasField(recJournalNum) {
			sr.setJournalNum(s.stJournalNum)
		}
		if !sr.hasField(recSequenceNum) {
			sr.setSequenceNum(s.stSeqNum)
		}

		if !sr.hasField(recComparer) {
			sr.setComparer([]byte("")) //todo 填充cmpr的名称
		}

	}
}

func (s *Session) markFileNum(f int64) {

	for {
		old, x := atomic.LoadInt64(&s.stJournalNum), f
		if old > x {
			x = old
		}
		if atomic.CompareAndSwapInt64(&s.stNextFileNum, old, x) {
			break
		}
	}

}

// 将memdb的内容持久化到sstable中, 并更新sessionrecord
func (s *Session) flushMemDb(rec *SessionRecord, memDB *memdb.MemDB) error {
	iter := memDB.NewIterator()
	defer iter.UnRef()

	// 生成sstable文件
	tFile, err := s.tableOpts.createFrom(iter)
	if err != nil {
		return err
	}

	rec.addTableFile(0, tFile)

	return nil
}

// 将rec更新到manifest文件, 并更新session相应的version
func (s *Session) commit(rec *SessionRecord) error {

	v := s.version()
	defer v.unRef()

	staging := v.newVersionStaging()
	staging.commit(rec)
	nv := staging.finish() // 产生新的version

	var err error

	if s.manifest == nil { // todo 判断manifest文件的大小超过一定限制就启用新的文件
		err = s.newManifest(rec, nv)
	} else {
		err = s.flushManifest(rec)
	}

	if err != nil {
		return err
	}

	// 更新或者写进去manifest成功后, 更新session
	s.setVersion(rec, nv)
	return nil
}

func (s *Session) flushManifest(rec *SessionRecord) error {

	writer := s.manifestWriter
	s.fillRecord(rec, false)

	err := rec.encode(writer)
	if err != nil {
		return err
	}

	err = writer.Sync()
	if err != nil {
		return err
	}

	s.commitRecord(rec)

	return nil
}

func (s *Session) newManifest(rec *SessionRecord, nv *Version) (err error) {

	fd := storage.FileDesc{Type: storage.FileTypeManifest, Num: int(s.allocNextNum())}

	writer, err := s.stor.Create(fd)
	if err != nil {
		return err
	}

	s.fillRecord(rec, true)
	nv.fillRecord(rec)
	manifest := journal.NewWriter(writer)

	defer func() {
		if err == nil {
			s.manifest = manifest
			if s.manifestWriter != nil {
				s.manifestWriter.Close()
			}

			if !s.manifestFd.Zero() {
				s.stor.Remove(s.manifestFd)
			}

			s.manifestWriter = writer
			s.manifestFd = fd
		}
	}()

	err = rec.encode(manifest)
	if err != nil {
		return err
	}

	err = writer.Sync()
	if err != nil {
		return err
	}

	err = s.stor.SetMeta(fd)
	if err != nil {
		return err
	}

	s.commitRecord(rec)

	return nil

}

func (s *Session) dupOptions(opt *Options) {
	s.Options = opt

	s.icmp = &iComparer{s.Options.GetCompare()}

}

func (s *Session) tLen(n int) int {
	s.vmu.Lock()
	defer s.vmu.Unlock()
	v := s.stVersion
	v.incRef()
	defer v.unRef()
	return s.stVersion.tLen(n)
}
