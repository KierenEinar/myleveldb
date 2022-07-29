package iter

import "myleveldb/utils"

type EmptyIterator struct {
	utils.BasicReleaser
	err error
}

func (ei *EmptyIterator) First() bool {
	return false
}

func (ei *EmptyIterator) Seek(key []byte) bool {
	return false
}

func (ei *EmptyIterator) Next() bool {
	return false
}

func (ei *EmptyIterator) Key() []byte {
	return nil
}

func (ei *EmptyIterator) Value() []byte {
	return nil
}

func NewEmptyIterator(err error) Iterator {
	ei := &EmptyIterator{
		err: err,
	}
	return ei
}
