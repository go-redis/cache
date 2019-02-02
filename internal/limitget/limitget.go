package limitget
// limitget is designed limit the number of Redis.Get.

import "sync"

// MaxConcurrency represents the total number of Redis.Get with all kinds of key.
var MaxConcurrency = 1000
//MaxGetNum represents the number of Redis.Get with same key at a time.
var MaxGetNum = uint8(2)

type Chans struct {
	sync.Mutex // protects m
	M          map[string]chan uint8
	maxConCh   chan uint8 //The channel is used for control the max concurrency number of Redis.Get
}

func (chs *Chans) GetChan(key string) (chan uint8, bool) {
	chs.Lock()
	defer chs.Unlock()
	ch, refreshing := chs.M[key]

	return ch, refreshing
}

func (chs *Chans) SetChan(key string) {
	chs.Lock()
	if chs.maxConCh == nil {
		chs.maxConCh = make(chan uint8, MaxConcurrency)
	}
	chs.Unlock()

	chs.maxConCh <- uint8(1)

	// We new a channel which represents the status of getting or not.
	ch := make(chan uint8, MaxGetNum-1)  // MaxGetNum-1>=0
	//MaxGetNum goroutine doesn't need to wait.
	for i := uint8(0); i < MaxGetNum-1; i++ {
		ch <- uint8(1)
	}

	chs.Lock()
	chs.M[key] = ch
	chs.Unlock()
}

func (chs *Chans) DeleteChan(key string) {
	chs.Lock()
	defer chs.Unlock()
	ch, _ := chs.GetChan(key)

	delete(chs.M, key)
	<-chs.maxConCh

	close(ch) //Notify channel to stop waiting
}
