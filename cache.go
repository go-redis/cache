package cache // import "gopkg.in/go-redis/cache.v4"

import (
	"errors"
	"log"
	"sync/atomic"
	"time"

	"gopkg.in/go-redis/cache.v4/lrucache"
	"gopkg.in/redis.v4"
)

const defaultExpiration = 3 * 24 * time.Hour

var ErrCacheMiss = errors.New("rediscache: cache miss")

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

	hits, misses int64
}

type Item struct {
	Key    string
	Object interface{}

	// Expiration is the cache expiration time.
	// Zero means the Item has no expiration time.
	Expiration time.Duration
}

func (cd *Codec) Set(item *Item) error {
	if item.Expiration != 0 && item.Expiration < time.Second {
		panic("Expiration can't be less than 1 second")
	}

	b, err := cd.Marshal(item.Object)
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

func (cd *Codec) Get(key string, v interface{}) error {
	b, err := cd.getBytes(key)
	if err == redis.Nil {
		atomic.AddInt64(&cd.misses, 1)
		return ErrCacheMiss
	} else if err != nil {
		log.Printf("cache: Get key=%s failed: %s", key, err)
		atomic.AddInt64(&cd.misses, 1)
		return err
	}

	if v != nil {
		if err := cd.Unmarshal(b, v); err != nil {
			log.Printf("cache: Unmarshal failed: %s", err)
			atomic.AddInt64(&cd.misses, 1)
			return err
		}
	}

	atomic.AddInt64(&cd.hits, 1)
	return nil
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
