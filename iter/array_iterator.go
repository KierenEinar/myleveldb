package iter

import (
	"errors"
	"myleveldb/utils"
)

type BasicArray interface {
	Len() int
	Search(key []byte) int
	Get(i int) Iterator
}

type arrayIteratorIndexer struct {
	utils.BasicReleaser
	array BasicArray
	pos   int
}

func (b *arrayIteratorIndexer) First() bool {

	if b.Released() {
		return false
	}

	if b.array.Len() == 0 {
		return false
	}

	b.pos = 0
	return true
}

func (b *arrayIteratorIndexer) Next() bool {

	if b.Released() {
		return false
	}

	b.pos++
	if n := b.array.Len(); n >= b.pos {
		b.pos = n
		return false
	}

	return true
}

func (b *arrayIteratorIndexer) Seek(key []byte) bool {

	n := b.array.Len()
	if n == 0 {
		return false
	}

	b.pos = b.array.Search(key)
	return b.pos < n
}

func (b *arrayIteratorIndexer) Get() Iterator {
	if n := b.array.Len(); b.pos < n {
		return b.array.Get(b.pos)
	}
	return NewEmptyIterator(errors.New("myLevelDb/arrayIteratorIndexer Get() eoi"))
}

func NewArrayIndexer(basicArray BasicArray) IteratorIndexer {
	return &arrayIteratorIndexer{
		array: basicArray,
		pos:   -1,
	}
}
