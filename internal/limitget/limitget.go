package limitget

// limitget is designed limit the number of Redis.Get.

import "sync"

// MaxConcurrency represents the total number of Redis.Get with all kinds of key.
var MaxConcurrency = 1000

type Chans struct {
	sync.Mutex // protects m
	M          map[string]bool
	maxConCh   chan uint8 //The channel is used for control the max concurrency number of Redis.Get
}

func (chs *Chans) LimitGet(key string) {
	chs.Lock()
	_, ok := chs.M[key]
	chs.Unlock()

	if !ok {
		chs.set(key)
	}
}

func (chs *Chans) set(key string) {
	chs.Lock()
	if chs.maxConCh == nil {
		chs.maxConCh = make(chan uint8, MaxConcurrency)
	}
	chs.Unlock()

	chs.maxConCh <- uint8(1)

	chs.Lock()
	chs.M[key] = true
	chs.Unlock()
}

func (chs *Chans) ReleaseGet(key string) {
	chs.Lock()
	defer chs.Unlock()
	_, ok := chs.M[key]
	if !ok {
		return
	}

	delete(chs.M, key)
	<-chs.maxConCh
}
