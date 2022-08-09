package collections

import (
	"math/rand"
	"testing"
	"time"
)

var maxHeapIntComparable = func(array *[]interface{}, i, j int) bool {

	iVal := (*array)[i].(int)
	jVal := (*array)[j].(int)
	return iVal > jVal
}

var minHeapIntComparable = func(array *[]interface{}, i, j int) bool {

	iVal := (*array)[i].(int)
	jVal := (*array)[j].(int)
	return iVal < jVal
}

func TestMaxHeap(t *testing.T) {

	heap := InitHeap(maxHeapIntComparable)

	s := rand.NewSource(time.Now().Unix())

	for i := 0; i < 10; i++ {
		heap.Push(rand.New(s).Int())
	}

	for !heap.Empty() {
		t.Logf("--- test heap ---, %d", heap.Pop())
	}
}

func TestMinHeap(t *testing.T) {

	heap := InitHeap(minHeapIntComparable)

	s := rand.NewSource(time.Now().Unix())

	for i := 0; i < 10; i++ {
		heap.Push(rand.New(s).Int())
	}

	for !heap.Empty() {
		t.Logf("--- test heap ---, %d", heap.Pop())
	}
}
