package cache

import (
	"math/rand"
	"time"

	"github.com/vmihailenco/go-tinylfu"
)

const (
	// MaxOffset specifies the max offset.
	MaxOffset = int64(10 * time.Second)

	// DefaultLFUSamples specifies the default lfu samples
	DefaultLFUSamples = 100000
)

// LocalCache interface.
type LocalCache interface {
	Set(key string, data []byte)
	Get(key string) ([]byte, bool)
	Del(key string)
}

type tinyLFU struct {
	rand   *rand.Rand
	lfu    *tinylfu.SyncT
	ttl    time.Duration
	offset int64
}

var _ LocalCache = (*tinyLFU)(nil)

type conf struct {
	src     rand.Source
	samples int
	offset  int64
}

func (c *conf) SetDefaults(offset int64) {
	c.src = rand.NewSource(time.Now().UnixNano())
	c.samples = DefaultLFUSamples
	c.setOffset(offset)
}

func (c *conf) setOffset(offset int64) {
	if offset > MaxOffset {
		c.offset = MaxOffset
	}

	c.offset = offset
}

// Option functional option type.
type Option func(*conf)

// UseRandomizedTTL change the offset (by default it is ttl / 10)
func UseRandomizedTTL(offset time.Duration) Option {
	return func(c *conf) {
		c.setOffset(int64(offset))
	}
}

// UseSamples change the lfu samples.
func UseSamples(samples int) Option {
	return func(c *conf) {
		c.samples = samples
	}
}

// UseRandomSource change the random source.
func UseRandomSource(src rand.Source) Option {
	return func(c *conf) {
		c.src = src
	}
}

// NewTinyLFU ctor.
func NewTinyLFU(size int, ttl time.Duration, opts ...Option) LocalCache {
	var c conf

	c.SetDefaults(int64(ttl) / 10)

	for _, opt := range opts {
		opt(&c)
	}

	return &tinyLFU{
		rand:   rand.New(c.src),
		lfu:    tinylfu.NewSync(size, c.samples),
		ttl:    ttl,
		offset: c.offset,
	}
}

func (c *tinyLFU) Set(key string, b []byte) {
	ttl := c.ttl
	if c.offset > 0 {
		ttl += time.Duration(c.rand.Int63n(c.offset))
	}

	c.lfu.Set(&tinylfu.Item{
		Key:      key,
		Value:    b,
		ExpireAt: time.Now().Add(ttl),
	})
}

func (c *tinyLFU) Get(key string) ([]byte, bool) {
	val, ok := c.lfu.Get(key)
	if !ok {
		return nil, false
	}

	b, _ := val.([]byte)
	return b, true
}

func (c *tinyLFU) Del(key string) {
	c.lfu.Del(key)
}
