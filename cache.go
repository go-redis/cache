package cache

import (
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"

	"github.com/go-redis/cache/internal/lrucache"
	"github.com/go-redis/cache/internal/singleflight"
)

var ErrCacheMiss = errors.New("cache: key is missing")

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

type Codec struct {
	Redis rediser

	localCache *lrucache.Cache

	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	group        singleflight.Group
	hits, misses int64
}

// UseLocalCache causes Codec to cache items in local LRU cache.
func (cd *Codec) UseLocalCache(maxLen int, expiration time.Duration) {
	cd.localCache = lrucache.New(maxLen, expiration)
}

// Set caches the item.
func (cd *Codec) Set(item *Item) error {
	if item.Expiration >= 0 && item.Expiration < time.Second {
		item.Expiration = time.Hour
	} else if item.Expiration == -1 {
		item.Expiration = 0
	}

	object, err := item.object()
	if err != nil {
		return err
	}

	b, err := cd.Marshal(object)
	if err != nil {
		log.Printf("cache: Marshal key=%q failed: %s", item.Key, err)
		return err
	}

	if cd.localCache != nil {
		cd.localCache.Set(item.Key, b)
	}

	err = cd.Redis.Set(item.Key, b, item.Expiration).Err()
	if err != nil {
		log.Printf("cache: Set key=%q failed: %s", item.Key, err)
	}
	return err
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

	if err := cd.Unmarshal(b, object); err != nil {
		log.Printf("cache: key=%q Unmarshal(%T) failed: %s", key, object, err)
		return err
	}

	return nil
}

func (cd *Codec) getBytes(key string, onlyLocalCache bool) ([]byte, error) {
	if cd.localCache != nil {
		v, ok := cd.localCache.Get(key)
		if ok {
			b, ok := v.([]byte)
			if ok {
				atomic.AddInt64(&cd.hits, 1)
				return b, nil
			}
		}
	}

	if onlyLocalCache {
		return nil, ErrCacheMiss
	}

	b, err := cd.Redis.Get(key).Bytes()
	if err != nil {
		atomic.AddInt64(&cd.misses, 1)
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		log.Printf("cache: Get key=%q failed: %s", key, err)
		return nil, err
	}

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
func (cd *Codec) Once(item *Item) (interface{}, error) {
	if cd.localCache != nil {
		if err := cd.getItemFast(item); err == nil {
			return item.Object, nil
		}
	}
	return cd.group.Do(item.Key, func() (interface{}, error) {
		if err := cd.getItem(item); err == nil {
			return item.Object, nil
		}

		obj, err := item.Func()
		if err != nil {
			return nil, err
		}

		cd.Set(&Item{
			Key:        item.Key,
			Object:     obj,
			Expiration: item.Expiration,
		})

		err = cd.getItem(item)
		if err != nil {
			return nil, err
		}
		return item.Object, nil
	})
}

func (cd *Codec) getItem(item *Item) error {
	return cd._getItem(item, false)
}

func (cd *Codec) getItemFast(item *Item) error {
	return cd._getItem(item, true)
}

func (cd *Codec) _getItem(item *Item, onlyLocalCache bool) error {
	if item.Object != nil {
		return cd.get(item.Key, item.Object, onlyLocalCache)
	} else {
		return cd.get(item.Key, &item.Object, onlyLocalCache)
	}
}

func (cd *Codec) Delete(key string) error {
	if cd.localCache != nil {
		cd.localCache.Delete(key)
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

func (cd *Codec) Hits() int {
	return int(atomic.LoadInt64(&cd.hits))
}

func (cd *Codec) Misses() int {
	return int(atomic.LoadInt64(&cd.misses))
}
