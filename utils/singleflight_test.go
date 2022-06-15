package utils

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGroup_Do(t *testing.T) {

	group := new(Group)
	wg := sync.WaitGroup{}
	wg.Add(100)

	key := "foo"
	for i := int64(0); i < 100; i++ {
		go func(idx int64) {
			defer func() {
				wg.Done()
			}()
			value, shared, err := group.Do(key, func() (value interface{}, err error) {
				return getById(idx), nil
			})
			t.Logf("value=%v, shared=%v, err=%v", value, shared, err)
		}(i)
	}
	wg.Wait()

}

func TestGroup_DoPanic(t *testing.T) {

	group := new(Group)
	wg := sync.WaitGroup{}
	wg.Add(100)

	key := "foo"
	for i := int64(0); i < 100; i++ {
		go func(idx int64) {
			defer func() {
				wg.Done()
			}()
			value, shared, err := group.Do(key, func() (value interface{}, err error) {

				if idx > 0 {
					panic("123")
				}

				return 100, nil

			})
			t.Logf("value=%v, shared=%v, err=%v", value, shared, err)
		}(i)
	}
	wg.Wait()

}

func TestGroup_DoChan(t *testing.T) {
	group := new(Group)
	key := "foo"

	wg := sync.WaitGroup{}
	wg.Add(100)

	for i := int64(0); i < 100; i++ {

		ctx, _ := context.WithTimeout(context.Background(), time.Second*5)

		go func(idx int64) {
			ch := group.DoChan(key, func() (value interface{}, err error) {

				select {}

				return getById(idx), nil
			})

			select {
			case data := <-ch:
				t.Logf("value=%v, shared=%v, err=%v", data.val, data.shared, data.err)
			case <-ctx.Done():
				t.Logf("执行过期了")
			}

			wg.Done()

		}(i)
	}

	wg.Wait()

}

func getById(id int64) interface{} {
	time.Sleep(time.Millisecond * 50)
	return fmt.Sprintf("id:%d, name:%s", id, "hello")
}
