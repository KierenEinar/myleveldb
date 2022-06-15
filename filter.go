package myleveldb

import (
	"myleveldb/filter"
)

type iFilter struct {
	filter.IFilter
}

func (iFilter iFilter) Contains(data []byte, key []byte) bool {
	return iFilter.IFilter.Contains(data, internalKey(key).uKey())
}

func (iFilter iFilter) NewFilterGenerator(bitsPerKey uint8) iFilterGenerator {
	return iFilterGenerator{iFilter.IFilter.NewFilterGenerator(bitsPerKey)}
}

type iFilterGenerator struct {
	filter.IFilterGenerator
}

func (iFilterGenerator iFilterGenerator) Add(key []byte) {
	iFilterGenerator.IFilterGenerator.Add(internalKey(key).uKey())
}
