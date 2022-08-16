package myleveldb

import (
	"myleveldb/sstable"
	"myleveldb/storage"
	"sort"
	"sync/atomic"
)

// Version 数据库某一时刻的状态
type Version struct {
	ref      int64
	id       int64    // vid
	levels   []tFiles // 每一层的sstable文件描述符
	session  *Session
	released bool

	// compaction 相关
	cScore int // compaction计算分数, 分数大于等于1即可开始compaction
	cLevel int // compaction level

}

func (ver *Version) newVersionStaging() *VersionStaging {
	return &VersionStaging{base: ver}
}

type tableScratch struct {
	added   map[int64]atRecord
	deleted map[int64]struct{}
}

// VersionStaging 类似于version_edit
type VersionStaging struct {
	base    *Version
	scratch []tableScratch // 对应每层level的增删
}

// 将version_edit的变更更新到VersionStaging中
func (vs *VersionStaging) commit(r *SessionRecord) {

	/***
	base level 举例

										/----/
	level1								| 01 |
										/----/

							/----/					/----/
	level2 					| 12 |					| 22 |
							/----/					/----/


	 				/----/					/----/
	level3			| 13 |					| 23 |
					/----/					/----/



	session record (1)

	新增    level4 14     24      level3 33

	session record (2)

	新增    level5 15  25  删除 level4 24 level3 23  删除level3 33

	**/

	// 执行添加操作
	for _, added := range r.atRecords {
		scratch := vs.getScratch(added.level)
		if scratch.added == nil {
			scratch.added = make(map[int64]atRecord)
		}
		scratch.added[int64(added.num)] = added
	}

	// 执行删除操作
	for _, deleted := range r.dlRecords {
		scratch := vs.getScratch(deleted.level)
		if scratch.deleted == nil {
			scratch.deleted = make(map[int64]struct{})
		}
		scratch.deleted[int64(deleted.num)] = struct{}{}
	}

	for idx, scratch := range vs.scratch {

		// 当前level只有添加或者只有删除, 不需要重新合并添加或者删除的
		// 这一层没有删除
		if len(scratch.deleted) == 0 || len(scratch.added) == 0 {
			continue
		}

		cloneAdded := make(map[int64]atRecord, len(scratch.added))
		for _, v := range scratch.added {
			cloneAdded[int64(v.num)] = v
		}

		cloneDeleted := make(map[int64]struct{}, len(scratch.deleted))
		for k := range scratch.deleted {
			cloneDeleted[k] = struct{}{}
		}

		for _, v := range scratch.added {
			if _, ok := scratch.deleted[int64(v.num)]; ok {
				delete(cloneAdded, int64(v.num))
				delete(cloneDeleted, int64(v.num))
			}
		}

		scratch.added = cloneAdded
		scratch.deleted = cloneDeleted
		vs.scratch[idx] = scratch
	}

}

func (vs *VersionStaging) finish() *Version {

	nv := vs.newVersion()

	levelNum := len(vs.base.levels)

	if len(vs.scratch) > levelNum {
		levelNum = len(vs.scratch)
	}

	newLevels := make([]tFiles, levelNum)

	for level := 0; level < levelNum; level++ {

		var scratch = vs.scratch[level]

		var baseLevels tFiles

		if level < len(vs.base.levels) {
			baseLevels = vs.base.levels[level]
		}

		if len(scratch.added) == 0 && len(scratch.deleted) == 0 {
			newLevels[level] = baseLevels
		}

		newTables := make(tFiles, 0, len(baseLevels)+len(scratch.added)-len(scratch.deleted))

		for _, v := range baseLevels {
			if _, ok := scratch.deleted[int64(v.fd.Num)]; ok {
				continue
			}

			if _, ok := scratch.added[int64(v.fd.Num)]; ok {
				continue
			}

			newTables = append(newTables, v)

		}

		if len(scratch.added) == 0 {
			newLevels[level] = newTables
			continue
		}

		for fdNum, atRecord := range scratch.added {
			newTables = append(newTables, tFile{
				fd: storage.FileDesc{
					Type: storage.FileTypeSSTable,
					Num:  int(fdNum),
				},
				size: int64(atRecord.size),
				min:  atRecord.min,
				max:  atRecord.max,
			})
		}

		if len(newTables) > 0 {
			if level == 0 {
				newTables.sortByNum()
			} else {
				newTables.sortByKey(vs.base.session.icmp)
			}
		}

		newLevels[level] = newTables

	}
	n := levelNum
	// 裁剪
	for ; n > 0 && len(newLevels[n-1]) == 0; n-- {

	}

	nv.levels = newLevels[:n]
	vs.scratch = vs.scratch[:0]
	return nv
}

func (vs *VersionStaging) getScratch(level int) *tableScratch {

	// 如果当前scratch的不够level的长度, 那么扩容到level的长度
	if level >= len(vs.scratch) {
		scratch := make([]tableScratch, level+1)
		copy(scratch, vs.scratch)
		vs.scratch = scratch
	}
	return &vs.scratch[level]
}

func (vs *VersionStaging) newVersion() *Version {
	ntVersionId := atomic.AddInt64(&vs.base.session.ntVersionId, 1)
	nv := &Version{
		id:      ntVersionId - 1,
		session: vs.base.session,
	}
	return nv
}

func (v *Version) fillRecord(s *SessionRecord) {
	s.resetAddRecord()
	for idx, level := range v.levels {
		for j := range level {
			s.addTableFile(idx, level[j])
		}
	}
}

func (v *Version) tLen(level int) int {
	if level >= len(v.levels) {
		return 0
	}
	return len(v.levels[level])
}

func (v *Version) get(ikey internalKey, noValue bool) (value []byte, err error) {

	var (
		// for level 0, since level 0 key can hop cross
		zfound     = false
		zkt        keyType
		zkey, zval []byte
		zseq       uint64
	)

	v.walkOverlapping(ikey, func(level int, tf tFile) bool {

		var (
			fkey internalKey
			fval []byte
		)

		if noValue {
			fkey, err = v.session.tableOpts.FindKey(tf, ikey)
		} else {
			fkey, fval, err = v.session.tableOpts.Find(tf, ikey)
		}

		if err != nil {
			if err == sstable.ErrNotFound {
				return true
			}
			return false
		}

		uk, seq, kt, err := parseInternalKey(fkey)

		if err == nil {

			if v.session.icmp.uCompare(uk, ikey.uKey()) != 0 {
				return true
			}

			if level == 0 {
				zfound = true
				if seq >= zseq {
					zseq = seq
					zkey = uk
					zval = fval
					zkt = kt
				}
			} else {
				if kt == keyTypeVal {
					value = append([]byte(nil), fval...)
					err = nil
				} else if kt == keyTypeDel {
					err = sstable.ErrNotFound
				} else {
					panic("myLeveldb/version get keytype invalid")
				}
				return false
			}
		}

		return true

	}, func() bool {

		if zfound {

			if zkt == keyTypeVal {
				value = append([]byte(nil), zval...)
				err = nil
			} else if zkt == keyTypeDel {
				err = sstable.ErrNotFound
			} else {
				panic("myLeveldb/version get keytype invalid")
			}

			return false
		}

		return true

	})

	return
}

func (v *Version) walkOverlapping(ikey internalKey, f func(level int, tf tFile) bool,
	lf func() bool) {
	ukey := ikey.uKey()
	icmp := v.session.icmp
	for level, tables := range v.levels {
		if level == 0 {
			for idx := range tables {
				table := tables[idx]
				if table.overlapped(icmp, ukey, ukey) {
					if !f(level, table) {
						return
					}
				}
			}
		} else {

			idx := sort.Search(len(tables), func(i int) bool {
				return icmp.Compare(tables[i].max, ikey) >= 0
			})
			if idx < len(tables) {
				if icmp.uCompare(ukey, tables[idx].min.uKey()) >= 0 {
					if !f(level, tables[idx]) {
						return
					}
				}
			}
		}

		if lf != nil && !lf() {
			return
		}

	}
	return

}
