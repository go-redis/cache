package cache // import "gopkg.in/go-redis/cache.v4"

import (
	"errors"
	"log"
	"sync/atomic"
	"time"

	"gopkg.in/go-redis/cache.v4/internal/singleflight"
	"gopkg.in/go-redis/cache.v4/lrucache"
	"gopkg.in/redis.v4"
)

const defaultExpiration = 3 * 24 * time.Hour

var ErrCacheMiss = errors.New("cache: keys is missing")

type rediser interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Del(keys ...string) *redis.IntCmd
}

type Codec struct {
	Redis rediser

	// Local LRU cache for super hot items.
	LocalCache *lrucache.Cache

	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	group        singleflight.Group
	hits, misses int64
}

type Item struct {
	Key    string
	Object interface{}

	// Func returns object to cache.
	Func func() (interface{}, error)

	// Expiration is the cache expiration time.
	// Zero means the Item has no expiration time.
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

// Set caches the item.
func (cd *Codec) Set(item *Item) error {
	if item.Expiration != 0 && item.Expiration < time.Second {
		panic("Expiration can't be less than 1 second")
	}

	object, err := item.object()
	if err != nil {
		return err
	}

	b, err := cd.Marshal(object)
	if err != nil {
		log.Printf("cache: Marshal failed: %s", err)
		return err
	}

	if cd.LocalCache != nil {
		cd.LocalCache.Set(item.Key, b)
	}

	err = cd.Redis.Set(item.Key, b, item.Expiration).Err()
	if err != nil {
		log.Printf("cache: Set key=%s failed: %s", item.Key, err)
	}
	return err
}

// Get gets the object for the given key.
func (cd *Codec) Get(key string, object interface{}) error {
	b, err := cd.getBytes(key)
	if err == redis.Nil {
		atomic.AddInt64(&cd.misses, 1)
		return ErrCacheMiss
	} else if err != nil {
		log.Printf("cache: Get key=%s failed: %s", key, err)
		atomic.AddInt64(&cd.misses, 1)
		return err
	}

	if object != nil {
		if err := cd.Unmarshal(b, object); err != nil {
			log.Printf("cache: Unmarshal failed: %s", err)
			atomic.AddInt64(&cd.misses, 1)
			return err
		}
	}

	atomic.AddInt64(&cd.hits, 1)
	return nil
}

// Do gets the item.Object for the given item.Key from the cache or
// executes, caches, and returns the results of the given item.Func,
// making sure that only one execution is in-flight for a given item.Key
// at a time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (cd *Codec) Do(item *Item) (interface{}, error) {
	if err := cd.Get(item.Key, item.Object); err == nil {
		return item.Object, nil
	}

	return cd.group.Do(item.Key, func() (interface{}, error) {
		obj, err := item.Func()
		if err != nil {
			return nil, err
		}

		item.Object = obj
		cd.Set(item)

		return obj, nil
	})
}

func (cd *Codec) getBytes(key string) ([]byte, error) {
	if cd.LocalCache != nil {
		v, ok := cd.LocalCache.Get(key)
		if ok {
			b, ok := v.([]byte)
			if ok {
				return b, nil
			}
		}
	}

	b, err := cd.Redis.Get(key).Bytes()
	if err == nil && cd.LocalCache != nil {
		cd.LocalCache.Set(key, b)
	}
	return b, err
}

func (cd *Codec) Delete(key string) error {
	if cd.LocalCache != nil {
		cd.LocalCache.Delete(key)
	}

	deleted, err := cd.Redis.Del(key).Result()
	if err != nil {
		log.Printf("cache: Del key=%s failed: %s", key, err)
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
