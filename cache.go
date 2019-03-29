package cache

import (
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"

	"github.com/WhisperRain/cache/internal/limitget"
	"github.com/WhisperRain/cache/internal/lrucache"
	"github.com/WhisperRain/cache/internal/singleflight"
)

var ErrCacheMiss = errors.New("cache: key is missing")
var errRedisLocalCacheNil = errors.New("cache: both Redis and LocalCache are nil")

type rediser interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Del(keys ...string) *redis.IntCmd
}

type Item struct {
	Key    string
	Object interface{}

	// Func returns object to be cached.
	Func func() (interface{}, error)

	// Expiration is the cache expiration time.
	// Default expiration is 1 hour.
	Expiration time.Duration
}

func (item *Item) object() (interface{}, error) {
	if item.Object != nil {
		return item.Object, nil
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

type Codec struct {
	Redis rediser

	localCache *lrucache.Cache

	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	group singleflight.Group
	chans limitget.Chans

	hits        uint64
	misses      uint64
	localHits   uint64
	localMisses uint64
}

func NewCodec(ring *redis.Ring,
	marshal func(interface{}) ([]byte, error),
	unmarshal func([]byte, interface{}) error) *Codec {

	codec := &Codec{
		Redis:     ring,
		chans:     limitget.Chans{MaxConCh: make(chan uint8, limitget.MaxConcurrency)},
		Marshal:   marshal,
		Unmarshal: unmarshal,
	}
	return codec
}

// UseLocalCache causes Codec to cache items in local LRU cache.
func (cd *Codec) UseLocalCache(maxLen int, expiration time.Duration) {
	cd.localCache = lrucache.New(maxLen, expiration)
}

// Set caches the item.
func (cd *Codec) Set(item *Item) error {
	_, err := cd.setItem(item)
	return err
}

func (cd *Codec) setItem(item *Item) ([]byte, error) {
	object, err := item.object()
	if err != nil {
		return nil, err
	}

	b, err := cd.Marshal(object)
	if err != nil {
		log.Printf("cache: Marshal key=%q failed: %s", item.Key, err)
		return nil, err
	}

	if cd.localCache != nil {
		cd.localCache.Set(item.Key, b)
	}

	if cd.Redis == nil {
		if cd.localCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return b, nil
	}

	err = cd.Redis.Set(item.Key, b, item.exp()).Err()
	if err != nil {
		log.Printf("cache: Set key=%q failed: %s", item.Key, err)
	}
	return b, err
}

// Exists reports whether object for the given key exists.
func (cd *Codec) Exists(key string) bool {
	return cd.Get(key, nil) == nil
}

// Get gets the object for the given key.
func (cd *Codec) Get(key string, object interface{}) error {
	return cd.get(key, object, false)
}

func (cd *Codec) get(key string, object interface{}, onlyLocalCache bool) error {
	b, err := cd.getBytes(key, onlyLocalCache)
	if err != nil {
		return err
	}

	if object == nil || len(b) == 0 {
		return nil
	}

	err = cd.Unmarshal(b, object)
	if err != nil {
		log.Printf("cache: key=%q Unmarshal(%T) failed: %s", key, object, err)
		return err
	}

	return nil
}

func (cd *Codec) getBytes(key string, onlyLocalCache bool) ([]byte, error) {
	if cd.localCache != nil {
		b, ok := cd.localCache.Get(key)
		if ok {
			atomic.AddUint64(&cd.localHits, 1)
			return b, nil
		}
		atomic.AddUint64(&cd.localMisses, 1)
	}

	if onlyLocalCache {
		return nil, ErrCacheMiss
	}
	if cd.Redis == nil {
		if cd.localCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return nil, ErrCacheMiss
	}

	cd.chans.LimitGet()
	defer func() {
		cd.chans.ReleaseGet()
	}()
	//if data expires, we need refresh data . At that time, the time cost may be long enough
	b, err := cd.Redis.Get(key).Bytes()

	if err != nil {
		atomic.AddUint64(&cd.misses, 1)
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		log.Printf("cache: Get key=%q failed: %s", key, err)
		return nil, err
	}
	atomic.AddUint64(&cd.hits, 1)

	if cd.localCache != nil {
		cd.localCache.Set(key, b)
	}
	return b, nil
}

// Once gets the item.Object for the given item.Key from the cache or
// executes, caches, and returns the results of the given item.Func,
// making sure that only one execution is in-flight for a given item.Key
// at a time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (cd *Codec) Once(item *Item) error {
	b, cached, err := cd.getSetItemBytesOnce(item)
	if err != nil {
		return err
	}

	if item.Object == nil || len(b) == 0 {
		return nil
	}

	err = cd.Unmarshal(b, item.Object)
	if err != nil {
		log.Printf("cache: key=%q Unmarshal(%T) failed: %s", item.Key, item.Object, err)
		if cached {
			_ = cd.Delete(item.Key)
			return cd.Once(item)
		} else {
			return err
		}
	}

	return nil
}

func (cd *Codec) getSetItemBytesOnce(item *Item) (b []byte, cached bool, err error) {
	if cd.localCache != nil {
		b, err := cd.getItemBytesFast(item)
		if err == nil {
			return b, true, nil
		}
	}

	obj, err := cd.group.Do(item.Key, func() (interface{}, error) {
		b, err := cd.getItemBytes(item)
		if err == nil {
			cached = true
			return b, nil
		}

		obj, err := item.Func()
		if err != nil {
			return nil, err
		}

		b, err = cd.setItem(&Item{
			Key:        item.Key,
			Object:     obj,
			Expiration: item.Expiration,
		})
		if b != nil {
			// Ignore error if we have the result.
			return b, nil
		}
		return nil, err
	})
	if err != nil {
		return nil, false, err
	}
	return obj.([]byte), cached, nil
}

func (cd *Codec) getItemBytes(item *Item) ([]byte, error) {
	return cd.getBytes(item.Key, false)
}

func (cd *Codec) getItemBytesFast(item *Item) ([]byte, error) {
	return cd.getBytes(item.Key, true)
}

func (cd *Codec) Delete(key string) error {
	if cd.localCache != nil {
		cd.localCache.Delete(key)
	}

	if cd.Redis == nil {
		if cd.localCache == nil {
			return errRedisLocalCacheNil
		}
		return nil
	}

	deleted, err := cd.Redis.Del(key).Result()
	if err != nil {
		log.Printf("cache: Del key=%q failed: %s", key, err)
		return err
	}
	if deleted == 0 {
		return ErrCacheMiss
	}
	return nil
}

type Stats struct {
	Hits        uint64
	Misses      uint64
	LocalHits   uint64
	LocalMisses uint64
}

// Stats returns cache statistics.
func (cd *Codec) Stats() *Stats {
	stats := Stats{
		Hits:   atomic.LoadUint64(&cd.hits),
		Misses: atomic.LoadUint64(&cd.misses),
	}
	if cd.localCache != nil {
		stats.LocalHits = atomic.LoadUint64(&cd.localHits)
		stats.LocalMisses = atomic.LoadUint64(&cd.localMisses)
	}
	return &stats
}
