package collections

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	namespace = 100001
)

// 测试简单的get
func Test_lruMap_get(t *testing.T) {

	lruMap := newLruMap()
	key := []byte("foo")
	added, node := lruMap.get(namespace, hash32(key), key, true)

	assert.True(t, added)
	assert.NotNil(t, node)
}

// 测试扩容
func Test_lruMap_grow(t *testing.T) {

	lruMap := newLruMap()

	keys := make([][]byte, 18)
	for idx := range keys {
		keys[idx] = []byte(fmt.Sprintf("%x", rand.Int()))
		hash32 := 400004
		added, node := lruMap.get(namespace, uint32(hash32), keys[idx], true)
		assert.True(t, added)
		assert.NotNil(t, node)
	}

}

func BenchmarkLRUCache_concurrent_write(b *testing.B) {
	lruMap := newLruMap()

	wg := sync.WaitGroup{}
	wg.Add(1000)

	for i := 0; i < 1000; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				key := []byte(fmt.Sprintf("%d", idx*1000+j))
				hash32 := hash32(key)
				added, node := lruMap.get(namespace, hash32, key, true)
				// t.Logf("add = %v, node=%#v", added, node)
				assert.True(b, added)
				assert.NotNil(b, node)
			}
		}(i)
	}

	wg.Wait()

	b.Logf("%#v", lruMap)
}

// 测试高并发的写
func Test_lruMap_concurrent_write(t *testing.T) {

	lruMap := newLruMap()

	wg := sync.WaitGroup{}
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				key := []byte(fmt.Sprintf("%d", idx*1000+j))
				hash32 := hash32(key)
				added, node := lruMap.get(namespace, hash32, key, true)
				// t.Logf("add = %v, node=%#v", added, node)
				assert.True(t, added)
				assert.NotNil(t, node)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("%#v", lruMap)

}

// 测试缩容
func Test_lruMap_shrink(t *testing.T) {

	lruMap := newLruMap()

	keys := make([][]byte, 2000)

	for idx := range keys {
		keys[idx] = []byte(fmt.Sprintf("%x", idx))
		hash32 := hash32(keys[idx])
		added, node := lruMap.get(namespace, hash32, keys[idx], true)
		assert.True(t, added)
		assert.NotNil(t, node)
	}

	for idx := range keys {
		hash32 := hash32(keys[idx])
		added, node := lruMap.get(namespace, hash32, keys[idx], false)
		assert.False(t, added)
		assert.NotNil(t, node)
		lruMap.delete(namespace, hash32, keys[idx])
		added, node = lruMap.get(namespace, hash32, keys[idx], false)
		assert.False(t, added)
		assert.Nil(t, node)
	}

	t.Logf("lrumap = %#v", lruMap)

}
