package iter

import "myleveldb/utils"

type IteratorIndexer interface {
	CommonIterator
	Get() Iterator
}

type indexedIterator struct {
	utils.BasicReleaser
	index IteratorIndexer
	data  Iterator
}

func (i *indexedIterator) setData() {
	if i.data != nil {
		i.data.UnRef()
	}

	i.data = i.index.Get()
}

func (i *indexedIterator) clearData() {
	if i.data != nil {
		i.data.UnRef()
	}
	i.data = nil
}

func (i *indexedIterator) First() bool {

	if i.Released() {
		return false
	}
	switch {
	case i.index.First():
		i.setData()
	default:
		i.clearData()
		return false
	}
	return i.Next()
}

func (i *indexedIterator) Next() bool {

	switch {

	case i.data != nil && !i.data.Next():
		i.clearData()
		fallthrough
	case i.data == nil:
		if !i.index.Next() {
			return false
		}
		i.setData()
		return i.Next()
	}

	return true
}

func (i *indexedIterator) Seek(key []byte) bool {

	if !i.index.Seek(key) {
		i.clearData()
		return false
	}
	i.setData()

	if !i.data.Seek(key) {
		i.clearData()
		return i.Next()
	}

	return true

}

func (i *indexedIterator) UnRef() {
	i.clearData()
	i.index.UnRef()
	i.BasicReleaser.UnRef()
}

func (i *indexedIterator) Key() []byte {
	if i.data != nil {
		return i.data.Key()
	}
	return nil
}

func (i *indexedIterator) Value() []byte {
	if i.data != nil {
		return i.data.Value()
	}
	return nil
}

func NewIndexedIterator(index IteratorIndexer) Iterator {
	return &indexedIterator{
		index: index,
	}
}
