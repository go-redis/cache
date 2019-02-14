package cache

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"

	"github.com/go-redis/cache/internal/lrucache"
	"github.com/go-redis/cache/internal/singleflight"
)

var ErrCacheMiss = errors.New("cache: key is missing")
var errRedisLocalCacheNil = errors.New("cache: both Redis and LocalCache are nil")

type rediser interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Del(keys ...string) *redis.IntCmd
	Pipeline() redis.Pipeliner
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

type MGetArgs struct {
	// Keys to load
	Keys []string

	// A destination object which must be a key-value map
	Dst interface{}

	// Func returns a map of objects which corresponds to the provided cache keys
	ObjByCacheKeyLoader func(keysToLoad []string) (map[string]interface{}, error)

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

	defaultRedisExpiration time.Duration

	localCache *lrucache.Cache

	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	group singleflight.Group

	hits        uint64
	misses      uint64
	localHits   uint64
	localMisses uint64
}

// UseLocalCache causes Codec to cache items in local LRU cache.
func (cd *Codec) UseLocalCache(maxLen int, expiration time.Duration) {
	cd.localCache = lrucache.New(maxLen, expiration)
}

func (cd *Codec) SetDefaultRedisExpiration(expiration time.Duration) {
	cd.defaultRedisExpiration = expiration
	cd.ensureDefaultExp()
}

// Set caches the item.
func (cd *Codec) Set(items ...*Item) error {
	if len(items) == 1 {
		_, err := cd.setItem(items[0])
		return err
	} else if len(items) > 1 {
		return cd.mSetItems(items)
	}
	return nil
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

	err = cd.Redis.Set(item.Key, b, cd.exp(item.Expiration)).Err()
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

func (cd *Codec) MGet(dst interface{}, keys ...string) error {
	mapValue := reflect.ValueOf(dst)
	if mapValue.Kind() == reflect.Ptr {
		// get the value that the pointer mapValue points to.
		mapValue = mapValue.Elem()
	}
	if mapValue.Kind() != reflect.Map {
		return fmt.Errorf("dst must be a map instead of %v", mapValue.Type())
	}
	mapType := mapValue.Type()

	// get the type of the key.
	keyType := mapType.Key()
	if keyType.Kind() != reflect.String {
		return fmt.Errorf("dst key type must be a string, %v given", keyType.Kind())
	}

	elementType := mapType.Elem()
	// non-pointer values not supported yet
	if elementType.Kind() != reflect.Ptr {
		return fmt.Errorf("dst value type must be a pointer, %v given", elementType.Kind())
	}
	// get the value that the pointer elementType points to.
	elementType = elementType.Elem()

	// allocate a new map, if mapValue is nil.
	// @todo fix "reflect.Value.Set using unaddressable value"
	if mapValue.IsNil() {
		mapValue.Set(reflect.MakeMap(mapType))
	}

	res, err := cd.mGetBytes(keys)
	if err != nil {
		return err
	}

	for idx, data := range res {
		bytes, ok := data.([]byte)
		if !ok || bytes == nil {
			continue
		}
		elementValue := reflect.New(elementType)
		dstEl := elementValue.Interface()

		err := cd.Unmarshal(bytes, dstEl)
		if err != nil {
			return err
		}
		key := reflect.ValueOf(keys[idx])
		mapValue.SetMapIndex(key, reflect.ValueOf(dstEl))
	}

	return nil
}

func (cd *Codec) MGetAndCache(mItem *MGetArgs) error {
	err := cd.MGet(mItem.Dst, mItem.Keys ...)
	if err != nil {
		return err
	}
	m := reflect.ValueOf(mItem.Dst)
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}
	// map type is checked in the MGet function
	if m.Len() != len(mItem.Keys) {
		absentKeys := make([]string, len(mItem.Keys)-m.Len())
		idx := 0
		for _, k := range mItem.Keys {
			mapVal := m.MapIndex(reflect.ValueOf(k))
			if !mapVal.IsValid() {
				absentKeys[idx] = k
				idx++
			}
		}
		loadedData, loaderErr := mItem.ObjByCacheKeyLoader(absentKeys)
		if loaderErr != nil {
			return loaderErr
		}

		items := make([]*Item, len(loadedData))
		i := 0
		for key, d := range loadedData {
			m.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(d))
			items[i] = &Item{
				Key:    key,
				Object: d,
			}
			i++
		}
		return cd.Set(items ...)
	}
	return nil
}

func (cd *Codec) mSetItems(items []*Item) error {
	var pipeline redis.Pipeliner
	if cd.Redis != nil {
		pipeline = cd.Redis.Pipeline()
	}
	for _, item := range items {
		key := item.Key
		bytes, e := cd.Marshal(item.Object)
		if e != nil {
			return e
		}
		if cd.localCache != nil {
			cd.localCache.Set(key, bytes)
		}
		if pipeline != nil {
			pipeline.Set(key, bytes, cd.exp(item.Expiration))
		}
	}
	if pipeline != nil {
		_, err := pipeline.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

// mGetBytes actually returns [][]bytes in an order which corresponds to the provided keys
// an interface{} is used to not allocate intermediate structures
func (cd *Codec) mGetBytes(keys []string) ([]interface{}, error) {
	collectedData := make([]interface{}, len(keys))
	recordsMissedInLocalCache := len(keys)
	if cd.localCache != nil {
		for idx, k := range keys {
			var err error
			var d []byte
			d, err = cd.getBytes(k, true)
			if err == nil {
				collectedData[idx] = d
				recordsMissedInLocalCache--
			}
		}
	}

	if cd.Redis != nil && recordsMissedInLocalCache > 0 {
		pipeline := cd.Redis.Pipeline()
		for idx, b := range collectedData {
			if b == nil {
				// the pipeline result is stored here to be able not to store indexes for non-local keys
				collectedData[idx] = pipeline.Get(keys[idx])
			}
		}
		_, err := pipeline.Exec()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		hits := 0
		for idx, content := range collectedData {
			if redisResp, ok := content.(*redis.StringCmd); ok {
				data, err := redisResp.Result()
				if err == redis.Nil {
					collectedData[idx] = nil
					continue
				}
				if err != nil {
					return nil, err
				}
				collectedData[idx] = []byte(data)
				hits++
			}
		}
		misses := recordsMissedInLocalCache - hits
		atomic.AddUint64(&cd.hits, uint64(hits))
		atomic.AddUint64(&cd.misses, uint64(misses))
	}

	return collectedData, nil
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

func (cd *Codec) exp(itemExp time.Duration) time.Duration {
	if itemExp < 0 {
		return 0
	}
	if itemExp < time.Second {
		cd.ensureDefaultExp()
		return cd.defaultRedisExpiration
	}
	return itemExp
}

func (cd *Codec) ensureDefaultExp() {
	if cd.defaultRedisExpiration < time.Second {
		cd.defaultRedisExpiration = time.Hour
	}
}

func (cd *Codec) Delete(keys ...string) error {
	if cd.localCache != nil {
		for _, key := range keys {
			cd.localCache.Delete(key)
		}

	}

	if cd.Redis == nil {
		if cd.localCache == nil {
			return errRedisLocalCacheNil
		}
		return nil
	}

	pipeline := cd.Redis.Pipeline()

	for _, key := range keys {
		pipeline.Del(key)
	}
	_, err := pipeline.Exec()
	return err
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
