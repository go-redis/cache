package cache

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/go-tinylfu"
)

type LocalCache interface {
	Set(key string, data []byte)
	Get(key string) ([]byte, bool)
	Del(key string)
}

type TinyLFU struct {
	mu     sync.Mutex
	rand   *rand.Rand
	lfu    *tinylfu.T
	ttl    time.Duration
	offset time.Duration

	statsEnabled bool
	hits         uint64
	misses       uint64
}

var _ LocalCache = (*TinyLFU)(nil)

func NewTinyLFU(size int, ttl time.Duration, statsEnabled bool) *TinyLFU {
	const maxOffset = 10 * time.Second

	offset := ttl / 10
	if offset > maxOffset {
		offset = maxOffset
	}

	return &TinyLFU{
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		lfu:          tinylfu.New(size, 100000),
		ttl:          ttl,
		offset:       offset,
		statsEnabled: statsEnabled,
	}
}

func (c *TinyLFU) UseRandomizedTTL(offset time.Duration) {
	c.offset = offset
}

func (c *TinyLFU) Set(key string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ttl := c.ttl
	if c.offset > 0 {
		ttl += time.Duration(c.rand.Int63n(int64(c.offset)))
	}

	c.lfu.Set(&tinylfu.Item{
		Key:      key,
		Value:    b,
		ExpireAt: time.Now().Add(ttl),
	})
}

func (c *TinyLFU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	val, ok := c.lfu.Get(key)
	if !ok {
		if c.statsEnabled {
			atomic.AddUint64(&c.misses, 1)
		}
		return nil, false
	}

	b := val.([]byte)
	if c.statsEnabled {
		atomic.AddUint64(&c.hits, 1)
	}
	return b, true
}

func (c *TinyLFU) Del(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lfu.Del(key)
}

//------------------------------------------------------------------------------

type StatsLocal struct {
	Hits   uint64
	Misses uint64
}

// StatsLocal returns local cache statistics.
func (c *TinyLFU) StatsLocal() *Stats {
	if !c.statsEnabled {
		return nil
	}
	return &Stats{
		Hits:   atomic.LoadUint64(&c.hits),
		Misses: atomic.LoadUint64(&c.misses),
	}
}
