package singleget

import "sync"

// Chans is designed save map[key]channel.
// It makes sure that only one getBytes is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
type Chans struct {
	sync.Mutex       // protects m
	M  map[string]chan uint8
}

func (chs *Chans)GetChan(key string) (chan uint8, bool) {
	chs.Lock()
	defer chs.Unlock()
	ch, refreshing := chs.M[key]

	return ch, refreshing
}

func  (chs *Chans)SetChan(key string, ch chan uint8) {
	chs.Lock()
	defer chs.Unlock()
	chs.M[key] = ch
}

func  (chs *Chans)DeleteChan(key string) {
	chs.Lock()
	defer chs.Unlock()
	delete(chs.M, key)
}
