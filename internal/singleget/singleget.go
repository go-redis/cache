package singleget

import "sync"

var refreshMutex sync.Mutex
var refresh map[string]chan uint8

func init() {
	refreshMutex = sync.Mutex{}
	refresh = make(map[string]chan uint8)
}

func GetChan(key string) (chan uint8, bool) {
	refreshMutex.Lock()
	defer refreshMutex.Unlock()
	ch, refreshing := refresh[key]

	return ch, refreshing
}

func SetChan(key string, ch chan uint8) {
	refreshMutex.Lock()
	defer refreshMutex.Unlock()
	refresh[key] = ch
}

func DeleteChan(key string) {
	refreshMutex.Lock()
	defer refreshMutex.Unlock()
	delete(refresh, key)
}
