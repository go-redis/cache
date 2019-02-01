package limitget

import "sync"

// limitget is designed limit the number of Redis.Get with same key at a time.
// The number could be any value above 0. But it's const number.It shouldn't be too big.
// It makes sure that only MaxGetNum Redis.Get is in-flight for a given key at a
// time. If a new one comes in, the duplicate caller waits for the
// old one to complete and receives the same results.
// Of course MaxGetNum can be modified easily in cache.go
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
