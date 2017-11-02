/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package waitgroup

import (
	"sync"
	"sync/atomic"
)

type WaitGroup struct {
	m       sync.Mutex
	counter int32
	ch      chan struct{}
}

func (wg *WaitGroup) Add(delta int) {
	v := atomic.AddInt32(&wg.counter, int32(delta))
	// Panic if negative
	if v < 0 {
		panic("sync: negative WaitGroup counter")
	} else if v == 0 {
		wg.m.Lock()
		if wg.ch != nil {
			close(wg.ch)
			wg.ch = nil
		}
		wg.m.Unlock()
	}
}

func (wg *WaitGroup) Done() {
	wg.Add(-1)
}

func (wg *WaitGroup) Wait() {
	if atomic.LoadInt32(&wg.counter) == 0 {
		return
	}
	wg.m.Lock()
	if wg.ch == nil {
		wg.ch = make(chan struct{})
	}
	wg.m.Unlock()
	// In case when counter=0, but wg.ch has not been initiated.
	if atomic.LoadInt32(&wg.counter) == 0 {
		return
	}
	<-wg.ch
}
