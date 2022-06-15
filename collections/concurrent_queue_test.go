package collections

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentQueue_Offer(t *testing.T) {

	queue := NewConcurrentQueue()

	wg := sync.WaitGroup{}
	groups := 10
	wg.Add(groups)

	for i := 0; i < groups; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				val := fmt.Sprintf("%d-%d", idx, j)
				hash := idx*1e5 + j
				queue.Offer(int32(hash), val)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("len=%d", queue.Cap())

}

func TestConcurrentQueue_Pop(t *testing.T) {
	queue := NewConcurrentQueue()

	wg := sync.WaitGroup{}
	groups := 20
	wg.Add(groups)

	for i := 0; i < groups/2; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				val := fmt.Sprintf("%d-%d", idx, j)
				hash := idx*1e5 + j
				queue.Offer(int32(hash), val)
			}
		}(i)
	}

	for i := 0; i < groups/2; i++ {
		go func(idx int) {
			defer wg.Done()
			count := 50
			missHint := 0
			for {
				if count == 0 {
					break
				}
				val := queue.Pop()
				if val != nil {
					fmt.Println(val)
					count--
				} else {
					missHint++
				}
			}

			fmt.Printf("misshint=%d\n", missHint)

		}(i)
	}

	wg.Wait()

	t.Logf("len=%d", queue.Cap())
}
