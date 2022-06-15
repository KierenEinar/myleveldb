package collections

import (
	"bytes"
	"fmt"
	"math/rand"
	"myleveldb/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	cmp = func(a, b interface{}) int {
		return bytes.Compare(a.([]byte), b.([]byte))
	}

	pool = utils.NewBytePool(1 << 20)
)

func BenchmarkLLRBTree_Put(t *testing.B) {
	tree := NewLLRBTree(1<<22, cmp, pool)

	for idx := 0; idx < 1000; idx++ {
		key := []byte(fmt.Sprintf("key-%x", idx))
		value := []byte(fmt.Sprintf("value-%x", idx))
		err := tree.Put(key, value)
		assert.Nil(t, err)

		value1, err1 := tree.Get(key)
		assert.Nil(t, err1)
		assert.EqualValues(t, value1, value)
	}

	tree.DebugIterString()

	_ = tree.Close()
}

func BenchmarkLLRBTree_Set(t *testing.B) {
	tree := NewLLRBTree(1<<22, cmp, pool)

	for idx := 0; idx < 1000; idx++ {
		key := []byte(fmt.Sprintf("key-%x", idx))
		value := []byte(fmt.Sprintf("value-%x", idx))
		err := tree.Set(key, value)
		assert.Nil(t, err)

		value1, err1 := tree.Get(key)
		assert.Nil(t, err1)
		assert.EqualValues(t, value1, value)
	}

	tree.DebugIterString()

	_ = tree.Close()

}

func TestLLRBTree_Put(t *testing.T) {

	tree := NewLLRBTree(1<<22, cmp, pool)

	for idx := uint8(0); idx < uint8(10); idx++ {
		key := []byte(fmt.Sprintf("key-%x", idx))
		value := []byte(fmt.Sprintf("value-%x", idx))
		err := tree.Put(key, value)
		assert.Nil(t, err)

		value1, err1 := tree.Get(key)
		assert.Nil(t, err1)
		assert.EqualValues(t, value1, value)
	}

	tree.DebugIterString()

	ge, err := tree.FindGE([]byte("key-55"))
	assert.Nil(t, err)
	assert.EqualValues(t, ge, []byte("key-6"))
	t.Logf("ge=%s", ge)

	ge, err = tree.FindGE([]byte("key-71"))
	assert.Nil(t, err)
	assert.EqualValues(t, ge, []byte("key-8"))
	t.Logf("ge=%s", ge)

	ge, err = tree.FindGE([]byte("key-23"))
	assert.Nil(t, err)
	assert.EqualValues(t, ge, []byte("key-3"))
	t.Logf("ge=%s", ge)

	ge, err = tree.FindGE([]byte("key-77"))
	assert.Nil(t, err)
	assert.EqualValues(t, ge, []byte("key-8"))
	t.Logf("ge=%s", ge)

	ge, err = tree.FindGE([]byte("key-11"))
	assert.Nil(t, err)
	assert.EqualValues(t, ge, []byte("key-2"))
	t.Logf("ge=%s", ge)

	ge, err = tree.FindGE([]byte("key-1"))
	assert.Nil(t, err)
	assert.EqualValues(t, ge, []byte("key-1"))
	t.Logf("ge=%s", ge)

	_ = tree.Close()

	rbTree := NewLLRBTree(1<<22, cmp, pool)
	for i := 0; i < 20; i++ {
		randNum := rand.Int() % 100000
		key := []byte(fmt.Sprintf("key-%d", randNum))
		value := []byte(fmt.Sprintf("value-%d", randNum))

		err = rbTree.Put(key, value)
		assert.Nil(t, err)
	}

	rbTree.DebugIterString()

	_ = rbTree.Close()

}

func TestLLRBTreeIter_Next(t *testing.T) {
	tree := NewLLRBTree(1<<22, cmp, pool)

	for idx := 0; idx < 1000; idx++ {

		randNum := rand.Int() & 0xffff

		key := []byte(fmt.Sprintf("key-%d", randNum))
		value := []byte(fmt.Sprintf("value-%d", randNum))
		err := tree.Put(key, value)
		assert.Nil(t, err)

		value1, err1 := tree.Get(key)
		assert.Nil(t, err1)
		assert.EqualValues(t, value1, value)
	}

	iter := NewLLRBTreeIter(tree, nil)

	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
	}

	iter.UnRef()

	_ = tree.Close()

}

func TestLLRBTreeIter_Seek(t *testing.T) {
	tree := NewLLRBTree(1<<22, cmp, pool)

	for idx := 0; idx < 1000; idx++ {

		randNum := rand.Int() & 0xffff

		key := []byte(fmt.Sprintf("key-%d", randNum))
		value := []byte(fmt.Sprintf("value-%d", randNum))
		err := tree.Put(key, value)
		assert.Nil(t, err)

		value1, err1 := tree.Get(key)
		assert.Nil(t, err1)
		assert.EqualValues(t, value1, value)
	}

	iter := NewLLRBTreeIter(tree, nil)

	if iter.Seek([]byte("key-533")) {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
	}

	for iter.Next() {
		fmt.Printf("key=%s, value=%s\n", iter.Key(), iter.Value())
	}

	iter.UnRef()
	tree.Close()

}
