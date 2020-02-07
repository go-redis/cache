package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/go-redis/cache/v7/internal/singleflight"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/go-redis/redis/v7"
)

var ErrCacheMiss = errors.New("cache: key is missing")
var errRedisLocalCacheNil = errors.New("cache: both Redis and LocalCache are nil")

type rediser interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Del(keys ...string) *redis.IntCmd
}

type Item struct {
	Ctx context.Context

	Key   string
	Value interface{}

	// Func returns value to be cached.
	Func func() (interface{}, error)

	// Expiration is the cache expiration time.
	// Default expiration is 1 hour.
	Expiration time.Duration
}

func (item *Item) Context() context.Context {
	if item.Ctx == nil {
		return context.Background()
	}
	return item.Ctx
}

func (item *Item) value() (interface{}, error) {
	if item.Value != nil {
		return item.Value, nil
	}
	if item.Func != nil {
		return item.Func()
	}
	return nil, nil
}

func (item *Item) exp() time.Duration {
	if item.Expiration < 0 {
		return 0
	}
	if item.Expiration < time.Second {
		return time.Hour
	}
	return item.Expiration
}

//------------------------------------------------------------------------------

type Options struct {
	Redis rediser

	LocalCache    *fastcache.Cache
	LocalCacheTTL time.Duration

	StatsEnabled bool
}

type Cache struct {
	opt *Options

	group singleflight.Group

	hits   uint64
	misses uint64
}

func New(opt *Options) *Cache {
	return &Cache{
		opt: opt,
	}
}

// Set caches the item.
func (cd *Cache) Set(item *Item) error {
	obj, err := item.value()
	if err != nil {
		return err
	}
	_, err = cd.set(item.Context(), item.Key, obj, item.exp())
	return err
}

func (cd *Cache) set(
	ctx context.Context,
	key string,
	obj interface{},
	exp time.Duration,
) ([]byte, error) {
	b, err := msgpack.Marshal(obj)
	if err != nil {
		return nil, err
	}

	if cd.opt.LocalCache != nil {
		cd.opt.LocalCache.Set([]byte(key), b)
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return b, nil
	}

	return b, cd.opt.Redis.Set(key, b, exp).Err()
}

// Exists reports whether value for the given key exists.
func (cd *Cache) Exists(ctx context.Context, key string) bool {
	return cd.Get(ctx, key, nil) == nil
}

// Get gets the value for the given key.
func (cd *Cache) Get(ctx context.Context, key string, value interface{}) error {
	return cd.get(ctx, key, value)
}

func (cd *Cache) get(
	ctx context.Context,
	key string,
	value interface{},
) error {
	b, err := cd.getBytes(key)
	if err != nil {
		return err
	}

	if value == nil || len(b) == 0 {
		return nil
	}

	return msgpack.Unmarshal(b, value)
}

func (cd *Cache) getBytes(key string) ([]byte, error) {
	if cd.opt.LocalCache != nil {
		b, ok := cd.opt.LocalCache.HasGet(nil, []byte(key))
		if ok {
			return b, nil
		}
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return nil, ErrCacheMiss
	}

	b, err := cd.opt.Redis.Get(key).Bytes()
	if err != nil {
		atomic.AddUint64(&cd.misses, 1)
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		return nil, err
	}

	atomic.AddUint64(&cd.hits, 1)

	if cd.opt.LocalCache != nil {
		cd.opt.LocalCache.Set([]byte(key), b)
	}
	return b, nil
}

// Once gets the item.Value for the given item.Key from the cache or
// executes, caches, and returns the results of the given item.Func,
// making sure that only one execution is in-flight for a given item.Key
// at a time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (cd *Cache) Once(item *Item) error {
	b, cached, err := cd.getSetItemBytesOnce(item)
	if err != nil {
		return err
	}

	if item.Value == nil || len(b) == 0 {
		return nil
	}

	if err := msgpack.Unmarshal(b, item.Value); err != nil {
		if cached {
			_ = cd.Delete(item.Context(), item.Key)
			return cd.Once(item)
		}
		return err
	}

	return nil
}

func (cd *Cache) getSetItemBytesOnce(
	item *Item,
) (b []byte, cached bool, err error) {
	if cd.opt.LocalCache != nil && cd.opt.LocalCache.Has([]byte(item.Key)) {
		b, ok := cd.opt.LocalCache.HasGet(nil, []byte(item.Key))
		if ok {
			return b, true, nil
		}
	}

	v, err := cd.group.Do(item.Key, func() (interface{}, error) {
		b, err := cd.getBytes(item.Key)
		if err == nil {
			cached = true
			return b, nil
		}

		obj, err := item.Func()
		if err != nil {
			return nil, err
		}

		return cd.set(item.Context(), item.Key, obj, item.exp())
	})
	if err != nil {
		return nil, false, err
	}
	return v.([]byte), cached, nil
}

func (cd *Cache) Delete(ctx context.Context, key string) error {
	if cd.opt.LocalCache != nil {
		cd.opt.LocalCache.Del([]byte(key))
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return errRedisLocalCacheNil
		}
		return nil
	}

	deleted, err := cd.opt.Redis.Del(key).Result()
	if err != nil {
		return err
	}
	if deleted == 0 {
		return ErrCacheMiss
	}
	return nil
}

//------------------------------------------------------------------------------

type Stats struct {
	Hits        uint64
	Misses      uint64
	LocalHits   uint64
	LocalMisses uint64
}

// Stats returns cache statistics.
func (cd *Cache) Stats() *Stats {
	stats := Stats{
		Hits:   atomic.LoadUint64(&cd.hits),
		Misses: atomic.LoadUint64(&cd.misses),
	}
	return &stats
}
