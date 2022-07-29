package myleveldb

import (
	"myleveldb/comparer"
)

type iComparer struct {
	ucmp comparer.BasicComparer
}

func (ic iComparer) Compare(a, b []byte) int {

	// aaaa1100 aaaa1000
	r := ic.ucmp.Compare(internalKey(a).uKey(), internalKey(b).uKey())

	if r == 0 {
		if m, n := internalKey(a).num(), internalKey(b).num(); m > n {
			return 1
		} else if m < n {
			return -1
		}
	}
	return r
}
