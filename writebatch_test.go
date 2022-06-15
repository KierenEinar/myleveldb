package myleveldb

import (
	"sync"
	"testing"
)

func TestWriteMerge_Put(t *testing.T) {

	wm := NewWriteMerge()
	wg := sync.WaitGroup{}
	wg.Add(200)

	withBatch := func(batch *Batch) error {
		t.Logf("batchs %#v", batch.index)
		t.Logf("batchs len %d", len(batch.index))
		return nil
	}

	for i := 0; i < 200; i++ {
		go func() {

			defer wg.Done()

			for i := 0; i < 10; i++ {
				wm.Put(keyTypeVal, []byte("hello"), []byte("world"), withBatch)
			}
		}()
	}

	wg.Wait()

}
