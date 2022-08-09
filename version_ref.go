package myleveldb

import "time"

/**
本文件描述了多version引用的情况下, 如何对正在引用和已经没有引用的进行删除

fileRef 对文件的引用次数 map[int64]int64

type vTask struct {
 	vid int64 // version id
	files []tFiles
	create_time time.Time
}

type vDelta struct {
	vid int64 // version id
	added 	[]int64
	deleted []int64
}

VersionRef 正在被引用的版本 map[int64]*vTask, 如果version有变化的话, 会通过vDelta通知对应的版本修改
VersionRef[i+1] = VersionRef[i] + vDelta


Referenced 被引用的版本 map[int64]struct{}, 通过定时器将VersionRef修改为对文件的引用, 体现在fileRef上,
然后将VersionRef对应的vid删除, 如果version有变化的话, 也是通过vDelta通知对应的版本修改, 但是直接对vDelta的
增加或者修改做引用加减


Released 已经被析构的版本, map[int64]*vTask 存在于 Released 的版本绝对是从 VersionRef 变化过来的。
如果是 Referenced 的话, 那么也是通过直接对vTask的增加或者减少做文件引用计算操作


如何对 VersionRef 平铺为 fileRef 的引用

启动一个processTasks的函数, 该函数负责以下职责
				  delta1	  delta2	 delta3	    delta4      delta5
			   /         \	/       \  /       \  /         \ /       \
		//---------//---------//---------//---------//---------//---------//
		|   ref 1  |   ref 2  |   ref 3  |   ref 4  |   ref 5  |   ref 6  |
		//---------//---------//---------//---------//---------//---------//

next: 单调递增

当VersionRef数量超过一个阈值的时候, 会对ref从低数值到高数值逐步对VersionRef平铺成对文件的引用
当遇到next在Released中不存在的话, 就会跳出循环 goto processRelease(next), 如果processRelease成功的话,
next自增1

如果当前next在VersionRef中不存在的话, 就会跳出循环 goto processRelease(next), 如果processRelease成功的话,
next自增1

走到这说明在VersionRef已经存在了, 那么就直接走引用文件平铺操作。

*采用版本引用不会被自己平铺成对文件的引用是为了减少频繁的对文件删除。

重要申明

1. 当版本已经被平铺为引用文件时, 如果有产生vdelta, 那么需要将vdelta应用到版本变化中,
如果vdelta中有add, 那么对add的做引用+1, 如果有deleted, 那么对deleted的做引用减1
e.g.

			a 			-> delta		->b

		1 	2	3        -3  +4 	 1 	 2 	 4
如果不对delta做引用操作, 当b执行release的时候, 就会把4给释放掉, 造成bug


2. 当版本没有被平铺为引用文件时, 如果有产生release, 只需要把released引用为vDelta即可,
在processTasks中, 会按照version的顺序向后推进(这个很重要), 将release的vDelta直接做文件引用即可


**/

const (
	maxVersionRefCachedNum = 256             // 最多n个版本不需要平铺成文件引用
	maxCachedTime          = time.Minute * 5 // 一个version不被平铺成fileref的最小时长
)

// VersionRef 版本引用
type VersionRef struct {
	vid        int64     // 版本id
	files      []tFiles  // 版本引用的所有文件
	createTime time.Time // 版本创建时间
}

// VersionDelta 版本delta
type VersionDelta struct {
	vid     int64 // 版本id
	added   []int64
	deleted []int64
}

// VersionRelease 版本析构
type VersionRelease struct {
	vid   int64    // 版本id
	files []tFiles // 版本引用的所有文件
}

func (s *Session) refLoop() {

	var (
		fileRef          = make(map[int64]int64)         // 文件引用, key是file_num, value是引用次数
		versionRef       = make(map[int64]*VersionRef)   // 版本引用, key是vid, value是版本信息
		deltaRef         = make(map[int64]*VersionDelta) // 增量引用, key是vid, value是增量信息
		releasedRef      = make(map[int64]*VersionDelta) // 版本release, key是vid, value是版本变化信息
		versionToFileRef = make(map[int64]struct{})      // 版本已经被处理为单纯对文件的引用, vid是版本号

		next, last int64 // next是下一个ref被平铺成文件的file num, last是最后的版本号
	)

	addFileRef := func(fileNum int64, delta int64) int64 {
		ref := fileRef[fileNum]
		ref += delta
		if ref < 0 {
			panic("Session refLoop addFileNum negative")
		}

		if ref == 0 {
			delete(fileRef, fileNum)
			return 0
		}

		fileRef[fileNum] = ref
		return ref
	}

	applyDelta := func(delta *VersionDelta) {
		for _, v := range delta.added {
			addFileRef(v, 1)
		}
		for _, v := range delta.deleted {
			if addFileRef(v, -1) == 0 {
				//todo 删除ldb文件
			}
		}
	}

	timer := time.NewTimer(0)
	<-timer.C

	defer timer.Stop()

	// 处理版本引用
	processVersionRef := func() {

		timer.Reset(maxCachedTime)

		// 版本引用转文件引用
		for {
			// 如果release存在该版本, 那么跳出到releaseProcess即可
			if _, ok := releasedRef[next]; ok {
				goto releaseUnRef
			}

			// 如果ref不存在, 那么有可能是已经被referenced或者next是最后
			ver, ok := versionRef[next]
			if !ok {
				return
			}

			// 如果当前版本引用没有超过最大的cache数量并且当前版本引用
			if last-next > maxVersionRefCachedNum || time.Since(ver.createTime) > maxCachedTime {
				goto verFileRef
			} else {
				break
			}

		verFileRef:
			for _, files := range ver.files {
				for _, file := range files {
					addFileRef(int64(file.fd.Num), 1)
				}
			}
			delete(versionRef, next)
			deltas, ok := deltaRef[next]
			if ok {
				applyDelta(deltas)
				delete(deltaRef, next)
			}

			versionToFileRef[next] = struct{}{}
			next += 1
		}

	releaseUnRef:
		// 析构的版本引用转文件引用
		for {
			if delta, ok := releasedRef[next]; ok {
				if delta != nil {
					applyDelta(delta)
				}
				delete(releasedRef, next)
				delete(versionRef, next)
				next += 1
				continue
			}
			return
		}

	}

	for {

		processVersionRef()

		select {

		case ref := <-s.versionRefCh:
			if _, ok := versionRef[ref.vid]; ok {
				panic("myLeveldb:session/ duplicate version ref")
			}

			versionRef[ref.vid] = ref
			if ref.vid > last {
				last = ref.vid
			}

		case delta := <-s.versionDeltaCh:
			// 当前版本已经被平铺成对文件的引用
			if _, ok := versionRef[delta.vid]; !ok {
				if _, ok := versionToFileRef[delta.vid]; !ok {
					panic("myLeveldb:session/ delta change not found version file ref")
				}
				applyDelta(delta)
				continue
			}
			deltaRef[delta.vid] = delta

		case ref := <-s.versionRelCh:

			if _, ok := versionRef[ref.vid]; !ok {
				if _, ok := versionToFileRef[ref.vid]; !ok {
					panic("myLeveldb:session/ release not found version file ref")
				}

				for _, v := range ref.files {
					for _, f := range v {
						addFileRef(int64(f.fd.Num), -1)
					}
				}
				continue
			}
			releasedRef[ref.vid] = deltaRef[ref.vid]

		case <-timer.C:

		}

	}

}
