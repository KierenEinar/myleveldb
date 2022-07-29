package myleveldb

import (
	"encoding/binary"
	"errors"
	"io"
	"myleveldb/cache"
	"myleveldb/collections"
	"myleveldb/comparer"
	"myleveldb/iter"
	"myleveldb/sstable"
	"myleveldb/storage"
	"myleveldb/utils"
	"sort"
)

type tFile struct {
	fd       storage.FileDesc
	size     int64
	min, max internalKey
}

type tFiles []tFile

func (tf tFiles) sortByNum() {
	sort.Slice(tf, func(i, j int) bool {
		return tf[i].fd.Num < tf[j].fd.Num
	})
}

func (tf tFiles) sortByKey(icmp comparer.BasicComparer) {

	sort.Slice(tf, func(i, j int) bool {
		return icmp.Compare(tf[i].min, tf[j].min) < 0
	})

}

func (tf tFiles) NewIteratorIndexer(top *sstableOperation) iter.IteratorIndexer {
	return iter.NewArrayIndexer(&tFileArrayIndexer{
		tfs: tf,
		top: top,
	})
}

type tFileArrayIndexer struct {
	tfs tFiles
	top *sstableOperation
}

func (ti tFileArrayIndexer) Len() int {
	return len(ti.tfs)
}

func (ti tFileArrayIndexer) Get(i int) iter.Iterator {
	if i >= ti.Len() {
		return iter.NewEmptyIterator(errors.New("out of range bound"))
	}
	tf := ti.tfs[i]
	return ti.top.NewIterator(tf)
}

func (ti tFileArrayIndexer) Search(key []byte) int {
	return 0
}

// sstableOperation sstable 相关操作封装
type sstableOperation struct {
	s          *Session
	bPool      *utils.BytePool
	FileCache  *cache.NamespaceCache
	BlockCache *cache.NamespaceCache
}

func (sstOpt *sstableOperation) create(size int64) (*tWriter, error) {
	fd := storage.FileDesc{Type: storage.FileTypeSSTable, Num: int(sstOpt.s.allocNextNum())}
	w, err := sstOpt.s.stor.Create(fd)
	if err != nil {
		return nil, err
	}
	return &tWriter{
		fd:          fd,
		writer:      w,
		tableWriter: sstable.NewWriter(w, sstOpt.s.iFilter, sstOpt.bPool, size),
	}, nil
}

type tWriter struct {
	fd          storage.FileDesc
	writer      io.WriteCloser
	tableWriter *sstable.Writer
	first, last []byte
}

func (t *tWriter) append(key, value []byte) {
	if t.first == nil {
		t.first = key
	}
	t.tableWriter.Append(key, value)
	t.last = key
}

func (t *tWriter) finish() (*tFile, error) {

	err := t.tableWriter.Close()
	if err != nil {
		return nil, err
	}

	defer func() {
		t.writer.Close()
	}()

	return &tFile{
		fd:   t.fd,
		size: int64(t.tableWriter.BytesLen()),
		min:  t.first,
		max:  t.last,
	}, nil

}

func (sstOpt *sstableOperation) createFrom(iterator iter.Iterator) (*tFile, error) {

	tWriter, err := sstOpt.create(0)
	if err != nil {
		return nil, err
	}

	for iterator.Next() {
		tWriter.append(iterator.Key(), iterator.Value())
	}

	tf, err := tWriter.finish()
	if err != nil {
		return nil, err
	}

	return tf, nil

}

func (sstOpt *sstableOperation) open(t tFile) (*collections.LRUHandle, error) {

	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(t.fd.Num))
	lruHandle, err := sstOpt.FileCache.Get(key, func() (int64, collections.Value, collections.BucketNodeDeleterCallback, error) {

		fd, err := sstOpt.s.stor.Open(t.fd)
		if err != nil {
			return 0, nil, nil, err
		}

		reader, err := sstable.NewReader(fd, t.size, sstOpt.s.icmp, sstOpt.FileCache)
		if err != nil {
			fd.Close()
			return 0, nil, nil, err
		}

		return 1, reader, nil, nil
	})

	return lruHandle, err

}

func (sstOpt *sstableOperation) NewIterator(t tFile) iter.Iterator {
	ch, err := sstOpt.open(t)
	if err != nil {
		return iter.NewEmptyIterator(err)
	}
	iterator := ch.Value().(*sstable.Reader).NewIterator()
	iterator.SetReleaser(ch)
	return iterator
}
