package cache

import (
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/vmihailenco/go-tinylfu"
)

type LocalCache interface {
	Set(key string, data []byte)
	Get(key string) ([]byte, bool)
	Del(key string)
}

type TinyLFU struct {
	lfu *tinylfu.SyncT
	ttl time.Duration
}

var _ LocalCache = (*TinyLFU)(nil)

func NewTinyLFU(size int, ttl time.Duration) *TinyLFU {
	return &TinyLFU{
		lfu: tinylfu.NewSync(size, 100000),
		ttl: ttl,
	}
}

func (c *TinyLFU) Set(key string, b []byte) {
	c.lfu.Set(&tinylfu.Item{
		Key:      xxhash.Sum64String(key),
		Value:    b,
		ExpireAt: time.Now().Add(c.ttl),
	})
}

func (c *TinyLFU) Get(key string) ([]byte, bool) {
	val, ok := c.lfu.Get(xxhash.Sum64String(key))
	if !ok {
		return nil, false
	}

	b := val.([]byte)
	return b, true
}

func (c *TinyLFU) Del(key string) {
	c.lfu.Del(xxhash.Sum64String(key))
}
