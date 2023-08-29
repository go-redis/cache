package cache

import (
	"github.com/coocood/freecache"
	"sync"
	"time"
)

type Freecache struct {
	mu    sync.Mutex
	cache *freecache.Cache
	ttl   time.Duration
}

var _ LocalCache = (*Freecache)(nil)

func NewFreecache(size int, ttl time.Duration) *Freecache {
	return &Freecache{
		cache: freecache.NewCache(size),
		ttl:   ttl,
	}
}

func (c *Freecache) Set(key string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_ = c.cache.Set([]byte(key), b, int(c.ttl.Seconds()))
}

func (c *Freecache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	val, err := c.cache.Get([]byte(key))
	if err != nil {
		return nil, false
	}
	return val, true
}

func (c *Freecache) Del(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache.Del([]byte(key))
}
