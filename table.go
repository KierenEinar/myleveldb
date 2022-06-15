package myleveldb

import (
	"myleveldb/comparer"
	"myleveldb/storage"
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

func (tf tFiles) sortByKey(icmp comparer.Compare) {

	sort.Slice(tf, func(i, j int) bool {
		return icmp(tf[i].min, tf[j].min) < 0
	})

}
