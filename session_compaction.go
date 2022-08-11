package myleveldb

import (
	"myleveldb/iter"
	"sort"
)

/**
pickCompaction
问题思考: 如何选择input的sstable? 定位完input的sstable后, 有应该如何选择下一层level需要被合并的sstable文件?

*如何选择input的sstable?
	如果session存在cScore>=1的话, 那么必然存在cLevel(指需要被合并的最佳input层), ok, 这里就确定了哪一个level需要向下合并
	确定完input的level后，需要确定input的文件, 可以从当前session获取, 查找的方法是找出第一个sstable的max值大于当前层的compactPtr
	确定完首个sstable后, 需要判断input的文件, 如果不是第0层的话, 那就不需要扩大,
		第0层的话需要通过不间断的确定imin和imax的范围, 最终把所有范围在imin和imax的sstable确定

*如何选择下一层需要被compaction的sstable?
	既然input都确定好了, 那么可以通过imin和imax确定范围存在重叠的所有文件
** 重点
首轮确定完input和待compaction的文件后, 有一个优化的点,
需要重新找出所有文件里头imin和imax, 然后重新确定input的sstable文件, 如果发现input文件的长度有变化, 需要满足以下两个条件才可以确认使用新的input文件
	a. 根据input得出的imin和imax, 需要确保compaction的文件不会变化
	b. 重新计算得出的input+compaction的文件不能超过25个即可

*/

func (s *Session) pickCompaction() *Compaction {

	v := s.version()

	var sourceLevel int

	if v.cScore >= 1 {
		sourceLevel = v.cLevel
		vtf0 := v.levels[sourceLevel]
		var tf0 tFiles
		if sourceLevel >= 1 {
			ptr := s.getCompactPtr(sourceLevel)
			if ptr != nil {
				if idx := sort.Search(len(vtf0), func(i int) bool {
					if s.icmp.Compare(vtf0[i].max, ptr) > 0 {
						return true
					}
					return false
				}); idx < len(vtf0) {
					tf0 = append(tf0, vtf0[idx])
				}
			}
		}

		if len(tf0) == 0 {
			tf0 = append(tf0, vtf0[0])
		}
		return newCompaction(s, v, sourceLevel, tf0)
	}

	return nil

}

type Compaction struct {
	s                 *Session
	v                 *Version
	sourceLevel       int
	levels            [2]tFiles
	imin, imax        []byte
	gp                tFiles // 记录本次合并的min和max在level+2层的重叠sstable文件
	gpi               int    // 当前遍历到的gp index下标
	seenKey           bool
	gpOverlappedBytes int64 // 当前compaction新文件跟gp存在重叠的总大小
	maxGpOverlapped   int64 // 当前compaction新文件跟gp存在重叠的最大限制
	levelPtrs         []int //
}

func newCompaction(s *Session, v *Version, sourceLevel int, vtf0 tFiles) *Compaction {
	c := &Compaction{
		s:           s,
		v:           v,
		sourceLevel: sourceLevel,
		levels:      [2]tFiles{vtf0, nil},
		levelPtrs:   make([]int, len(v.levels)),
	}
	c.expand()
	return c
}

func (s *Session) getCompactPtr(level int) internalKey {
	if level >= len(s.compactPtrs) {
		return nil
	}
	return s.compactPtrs[level]
}

func (c *Compaction) expand() {

	compactionLimit := c.s.Options.GetCompactionLimit()

	sourceLevel := c.sourceLevel

	vt0 := c.v.levels[sourceLevel]
	vt1 := tFiles{}
	if sourceLevel+1 < len(c.v.levels) {
		vt1 = c.v.levels[sourceLevel+1]
	}

	tf0, tf1 := c.levels[0], c.levels[1]

	// 先获取t0的min和max区间
	imin, imax := tf0.getRange(c.s.icmp)

	// 如果input是level0的话, 需要扩大input文件, 因为leveldb规定了level0层所有文件key可以重叠
	if sourceLevel == 0 {
		// 重新推算level0的input
		tf0 = vt0.getOverlaps(c.s.icmp, imin.uKey(), imax.uKey(), sourceLevel == 0)
		// 重新计算出imin和imax
		imin, imax = tf0.getRange(c.s.icmp)
	}

	// 根据imin和imax计算出
	tf1 = vt1.getOverlaps(c.s.icmp, imin.uKey(), imax.uKey(), false)

	amin, amax := append(tf0, tf1...).getRange(c.s.icmp)

	// 扩大input
	if len(tf1) > 0 {
		exp0 := vt0.getOverlaps(c.s.icmp, amin.uKey(), amax.uKey(), true)
		if len(exp0) > len(tf0) && exp0.size()+tf1.size() < compactionLimit { // 确认可以扩大输入
			xmin, xmax := exp0.getRange(c.s.icmp)
			// 重新确认下tf1会不会发生改变
			exp1 := vt1.getOverlaps(c.s.icmp, imin.uKey(), imax.uKey(), true)
			if len(exp1) == len(tf1) {
				imin, imax = xmin, xmax
				amin, amax = append(exp0, exp1...).getRange(c.s.icmp)
				tf0 = exp0
			}
		}
	}

	// 记录合并后跟level+2重叠的sstable文件
	if level := sourceLevel + 2; level < len(c.levels) {
		c.gp = c.levels[level].getOverlaps(c.s.icmp, amin.uKey(), amax.uKey(), false)
	}

	c.imin, c.imax = imin, imax
	c.levels[0], c.levels[1] = tf0, tf1
}

func (c *Compaction) newIterator(so *sstableOperation) iter.Iterator {

	icap := make([]iter.Iterator, 0, len(c.levels))
	if c.sourceLevel == 0 {
		icap = make([]iter.Iterator, 0, len(c.levels[0])+1)
	}

	for idx, level := range c.levels {

		if c.sourceLevel+idx == 0 {
			for _, tf := range level {
				icap = append(icap, so.NewIterator(tf))
			}
		} else {
			icap = append(icap, iter.NewIndexedIterator(iter.NewArrayIndexer(tFileArrayIndexer{
				tfs:  level,
				top:  so,
				icmp: so.s.icmp,
			})))
		}
	}

	return iter.NewMergedIterator(icap)

}

func (c *Compaction) trivial() bool {

	if len(c.levels[0]) == 1 && len(c.levels[1]) == 0 && len(c.gp) < c.s.Options.GetCompactionTrivialGpFiles() {
		return true
	}

	return false
}

func (c *Compaction) shouldStopBefore(ukey []byte) bool {

	/**
		如果当前ukey跟gp重复的超过限制

				/---------------/----------------/------------------/
														|ikey
		/------------/------/-------/----------------/--------/----------/

	**/
	for i := c.gpi; i < len(c.gp); i++ {
		if !c.gp[i].after(c.s.icmp, ukey) {
			break
		}
		if c.seenKey {
			c.gpOverlappedBytes += c.gp[i].size
		}
	}

	c.seenKey = true

	if c.gpOverlappedBytes >= c.maxGpOverlapped {
		c.gpOverlappedBytes = 0
		return true
	}
	return false
}

func (c *Compaction) restore() {
	c.seenKey = false
	c.gpOverlappedBytes = 0
}

// 粗略的估算ukey是否在level+2及以上是否存在相同但是seq不同
func (c *Compaction) isBaseLevelForKey(ukey []byte) bool {

	for level := c.sourceLevel + 2; level < len(c.v.levels); level++ {
		ptr := c.levelPtrs[level]
		l := c.v.levels[level]
		for ptr < len(l) {
			if c.s.icmp.uCompare(l[ptr].max.uKey(), ukey) >= 0 {
				if c.s.icmp.uCompare(l[ptr].min.uKey(), ukey) <= 0 { // 存在重叠, 不能自增, 因为下一个key可能也重叠
					return false
				}
				break
			}
			c.levelPtrs[level]++ // 不存在重叠, 下一个也绝对不会重叠, 因此可以自增
		}
	}
	return true
}
