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

func (t tFile) overlapped(icmp *iComparer, umin, umax []byte) bool {
	return !t.after(icmp, umax) && !t.before(icmp, umin)
}

func (t tFile) after(icmp *iComparer, umax []byte) bool {
	return icmp.uCompare(t.min.uKey(), umax) > 0
}

func (t tFile) before(icmp *iComparer, umin []byte) bool {
	return icmp.uCompare(t.max.uKey(), umin) < 0
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

func (tf tFiles) getRange(icmp comparer.BasicComparer) (imin, imax internalKey) {
	i := 0
	for i < len(tf) {
		if icmp.Compare(tf[i].min, imin) < 0 {
			imin = tf[i].min
		}
		if icmp.Compare(tf[i].max, imax) > 0 {
			imax = tf[i].max
		}
	}
	return
}

func (tf tFiles) getOverlaps(icmp *iComparer, umin, umax []byte, overlapped bool) tFiles {

	var dst tFiles

	if len(tf) == 0 {
		return dst
	}

	if overlapped {
		i := 0
		for ; i < len(tf); i++ {
			t := tf[i]
			if t.overlapped(icmp, umin, umax) {
				reLoop := false
				if icmp.uCompare(t.min.uKey(), umin) < 0 {
					umin = t.min.uKey()
					i = 0
					reLoop = true
				}
				if icmp.uCompare(t.max.uKey(), umax) > 0 {
					umax = t.max.uKey()
					i = 0
					reLoop = true
				}
				if reLoop {
					dst = dst[:0]
					continue
				}
				dst = append(dst, t)
			}
		}

	} else { // 处理非0层的情况

		/**
					    umin									    umax
		范围				/-------------------------------------------/

		真实sstable:

		e.g.1		/------/                /--------/					/-------/


		e.g.2       /------/											/-------/


		e.g.3	    /------/			/--------/		/-----/	/---------/


		e.g.4	/---/


		e.g.5																/---/	/------/


		e.g.6				/---------------------------/

				**/

		var begin, end int
		n := len(tf)
		// 所有sstable文件的key范围不会重叠, 所以可以使用二分查找
		// 首先定位sstable第一个min在umin之后的, 如果它的上一个sstable的max仍在umin之后, 那也要算进来
		idx := sort.Search(n, func(i int) bool {
			return icmp.uCompare(tf[i].min, umin) > 0
		})

		if idx == 0 || idx == n {
			begin = idx
		} else if icmp.uCompare(tf[idx-1].min.uKey(), umin) >= 0 {
			begin = idx - 1
		} else {
			begin = idx
		}

		// 再定位sstable第一个max在umax之后的

		idx = sort.Search(n, func(i int) bool {
			return icmp.uCompare(tf[i].max, umax) > 0
		})

		// 说明所有的sstable文件中不存在max大于umax的文件
		if idx == n {
			end = idx
		} else if icmp.uCompare(tf[idx].min, umin) <= 0 {
			end = idx + 1 // 因为golang的切片不包括index, 所以需要做+1操作
		} else {
			end = idx
		}

		if end-begin <= 0 {
			return dst
		}

		dst = make(tFiles, end-begin)
		copy(dst, tf[begin:end])
	}

	return dst

}

func (tf tFiles) size() (s int64) {
	for _, v := range tf {
		s += v.size
	}
	return
}

func (tf tFiles) NewIteratorIndexer(top *sstableOperation) iter.IteratorIndexer {
	return iter.NewArrayIndexer(&tFileArrayIndexer{
		tfs: tf,
		top: top,
	})
}

type tFileArrayIndexer struct {
	tfs  tFiles
	top  *sstableOperation
	icmp comparer.BasicComparer
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
	n := len(ti.tfs)
	return sort.Search(n, func(i int) bool {
		return ti.icmp.Compare(ti.tfs[i].max, internalKey(key)) >= 0
	})
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

func (sstOpt *sstableOperation) Find(t tFile, ikey internalKey) (rkey internalKey, value []byte, err error) {
	ch, err := sstOpt.open(t)
	if err != nil {
		return nil, nil, err
	}
	defer ch.UnRef()
	reader := ch.Value().(*sstable.Reader)
	return reader.Find(ikey)
}

func (sstOpt *sstableOperation) FindKey(t tFile, ikey internalKey) (rkey internalKey, err error) {
	ch, err := sstOpt.open(t)
	if err != nil {
		return nil, err
	}
	defer ch.UnRef()
	reader := ch.Value().(*sstable.Reader)
	return reader.FindKey(ikey)
}
