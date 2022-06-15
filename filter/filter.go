package filter

import "bytes"

type IFilterGenerator interface {
	Add(key []byte)
	Generate(buf *bytes.Buffer)
}

type IFilter interface {
	Contains(data []byte, key []byte) bool
	NewFilterGenerator(bitsPerKey uint8) IFilterGenerator
	Name() string
}
