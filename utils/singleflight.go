package utils

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Result 执行的结果
type Result struct {
	val    interface{}
	err    error
	shared bool
}

type Call struct {
	dups uint32
	wg   sync.WaitGroup
	val  interface{}
	err  error

	chans []chan<- Result
}

type Group struct {
	once  sync.Once
	calls map[string]*Call
	lock  sync.Mutex
}

func (g *Group) Do(key string, fn func() (value interface{}, err error)) (
	val interface{}, shared bool, err error) {

	g.once.Do(func() {
		g.calls = make(map[string]*Call)
	})

	g.lock.Lock()
	if call, ok := g.calls[key]; ok {
		g.lock.Unlock()
		atomic.AddUint32(&call.dups, 1)
		call.wg.Wait()
		return call.val, atomic.LoadUint32(&call.dups) > 0, call.err
	}

	call := new(Call)
	g.calls[key] = call
	g.lock.Unlock()
	call.wg.Add(1)
	g.doCall(key, call, fn)
	return call.val, atomic.LoadUint32(&call.dups) > 0, call.err
}

func (g *Group) DoChan(key string, fn func() (value interface{}, err error)) <-chan Result {
	g.once.Do(func() {
		g.calls = make(map[string]*Call)
	})

	ch := make(chan Result, 1)
	g.lock.Lock()
	if call, ok := g.calls[key]; ok {
		call.chans = append(call.chans, ch)
		g.lock.Unlock()
		atomic.AddUint32(&call.dups, 1)
		return ch
	}
	call := new(Call)
	call.chans = append(call.chans, ch)
	g.calls[key] = call
	g.lock.Unlock()
	go g.doCall(key, call, fn)
	return ch
}

func (g *Group) doCall(key string, call *Call, fn func() (value interface{}, err error)) {

	defer func() {
		if r := recover(); r != nil {
			s := debug.Stack()
			if call.err != nil {
				call.err = fmt.Errorf("err=%v, panic-stack=%s", call.err, s)
			} else {
				call.err = fmt.Errorf("panic-stack=%s", s)
			}
		}

		if len(call.chans) == 0 {
			call.wg.Done()
		}

		g.lock.Lock()
		defer g.lock.Unlock()

		if len(call.chans) > 0 {
			for _, v := range call.chans {
				v <- Result{
					val:    call.val,
					err:    call.err,
					shared: atomic.LoadUint32(&call.dups) > 0,
				}
			}
		}

		delete(g.calls, key)

	}()

	call.val, call.err = fn()

}
