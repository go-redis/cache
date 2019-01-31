package singleget

import "sync"

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Chans struct {
	sync.Mutex       // protects m
	M  map[string]chan uint8 // lazily initialized
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
