package cache_test

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/vmihailenco/msgpack"

	"github.com/go-redis/cache"
)

func TestGinkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cache")
}

func perform(n int, cbs ...func(int)) {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(cb func(int), i int) {
				defer wg.Done()
				defer GinkgoRecover()

				cb(i)
			}(cb, i)
		}
	}
	wg.Wait()
}

var _ = Describe("Codec", func() {
	const key = "mykey"
	var obj *Object

	var codec *cache.Codec

	testCodec := func() {
		It("Gets and Sets nil", func() {
			err := codec.Set(&cache.Item{
				Key:        key,
				Expiration: time.Hour,
			})
			Expect(err).NotTo(HaveOccurred())

			err = codec.Get(key, nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(codec.Exists(key)).To(BeTrue())
		})

		It("Deletes key", func() {
			err := codec.Set(&cache.Item{
				Key:        key,
				Expiration: time.Hour,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(codec.Exists(key)).To(BeTrue())

			err = codec.Delete(key)
			Expect(err).NotTo(HaveOccurred())

			err = codec.Get(key, nil)
			Expect(err).To(Equal(cache.ErrCacheMiss))

			Expect(codec.Exists(key)).To(BeFalse())
		})

		It("Deletes several keys", func() {
			items := []*cache.Item{}
			keys := []string{}
			for i := 0; i < 10; i++ {
				k := fmt.Sprintf("key-to-delete-%d", i)
				items = append(items, &cache.Item{
					Key: k,
					Object: "data",
				})
				keys = append(keys, k)
			}
			err := codec.Set(items ...)
			Expect(err).NotTo(HaveOccurred())

			for _, item := range items {
				Expect(codec.Exists(item.Key)).To(BeTrue())
			}


			err = codec.Delete(keys ...)
			Expect(err).NotTo(HaveOccurred())

			for _, item := range items {
				err = codec.Get(item.Key, nil)
				Expect(err).To(Equal(cache.ErrCacheMiss))
				Expect(codec.Exists(item.Key)).To(BeFalse())
			}
		})

		It("Gets and Sets data", func() {
			err := codec.Set(&cache.Item{
				Key:        key,
				Object:     obj,
				Expiration: time.Hour,
			})
			Expect(err).NotTo(HaveOccurred())

			wanted := new(Object)
			err = codec.Get(key, wanted)
			Expect(err).NotTo(HaveOccurred())
			Expect(wanted).To(Equal(obj))

			Expect(codec.Exists(key)).To(BeTrue())
		})

		It("Gets and Sets multiple data", func() {
			objects := []*Object{}
			for i := 0; i < 10; i++ {
				objects = append(objects, &Object{
					Str: fmt.Sprintf("set-get-%d", i),
					Num: i,
				})
			}

			for _, obj := range objects {
				err := codec.Set(&cache.Item{
					Key:        obj.Str,
					Object:     obj,
					Expiration: time.Hour,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			for _, obj := range objects {
				wanted := new(Object)
				err := codec.Get(obj.Str, wanted)
				Expect(err).NotTo(HaveOccurred())
				Expect(wanted).To(Equal(obj))

				Expect(codec.Exists(obj.Str)).To(BeTrue())
			}
		})

		It("Sets and MGets data", func() {
			dataToCache := map[string]*Object{}
			keys := []string{}
			for i := 0; i <= 10; i++ {
				key := fmt.Sprintf("mget-key-%d", i)
				keys = append(keys, key)
				dataToCache[key] = &Object{
					Str: fmt.Sprintf("mget-obj-%d", i),
					Num: i,
				}
			}
			for k, d := range dataToCache {
				err := codec.Set(&cache.Item{
					Key:        k,
					Object:     d,
					Expiration: time.Hour,
				})
				Expect(err).NotTo(HaveOccurred())
				//time.Sleep(5 * time.Second)
			}

			dstMap := map[string]*Object{}
			keysToLoad := []string{}
			keysToLoad = append(keysToLoad, keys ...)
			keysToLoad = append(keysToLoad, "absent-key", "non-exists-key")
			shuffle(keysToLoad)
			err := codec.MGet(dstMap, keys ...)
			Expect(err).NotTo(HaveOccurred())

			Expect(dstMap).To(Equal(dataToCache))

			for _, k := range keys {
				Expect(codec.Exists(k)).To(BeTrue())
			}
		})

		It("MGetAndCache data", func() {
			keys := []string{}
			availableObjects := map[string]*Object{}
			for i := 0; i < 10; i ++ {
				key := fmt.Sprintf("mget-n-cache-%d", i)
				keys = append(keys, key)
				availableObjects[key] = &Object{
					Str: fmt.Sprintf("mget-n-cache-obj-%d", i),
					Num: i,
				}
			}
			mItem := &cache.MGetArgs{
				Keys: keys,
				Dst:  map[string]*Object{},
				ObjByCacheKeyLoader: func(keysToLoad []string) (map[string]interface{}, error) {
					m := map[string]interface{}{}
					for _, k := range keysToLoad {
						m[k] = availableObjects[k]
					}
					return m, nil
				},
			}
			err := codec.MGetAndCache(mItem)
			Expect(err).NotTo(HaveOccurred())

			mGetMap := map[string]*Object{}
			err = codec.MGet(mGetMap, keys ...)
			Expect(err).NotTo(HaveOccurred())

			Expect(mItem.Dst).To(Equal(availableObjects))
			Expect(mItem.Dst).To(Equal(mGetMap))
		})

		It("MSet data", func() {
			keys := []string{}
			availableObjects := map[string]*Object{}
			for i := 0; i < 10; i ++ {
				key := fmt.Sprintf("mset-key-%d", i)
				keys = append(keys, key)
				availableObjects[key] = &Object{
					Str: fmt.Sprintf("mset-obj-%d", i),
					Num: i,
				}
			}

			items := []*cache.Item{}
			for k, v := range availableObjects {
				items = append(items, &cache.Item{
					Key:    k,
					Object: v,
				})
			}

			err := codec.Set(items ...)
			Expect(err).NotTo(HaveOccurred())

			mGetMap := map[string]*Object{}
			err = codec.MGet(mGetMap, keys ...)
			Expect(err).NotTo(HaveOccurred())

			Expect(availableObjects).To(Equal(mGetMap))
		})

		Describe("Once func", func() {
			It("calls Func when cache fails", func() {
				err := codec.Set(&cache.Item{
					Key:    key,
					Object: "*",
				})
				Expect(err).NotTo(HaveOccurred())

				var got bool
				err = codec.Get(key, &got)
				Expect(err).To(MatchError("msgpack: invalid code=a1 decoding bool"))

				err = codec.Once(&cache.Item{
					Key:    key,
					Object: &got,
					Func: func() (interface{}, error) {
						return true, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeTrue())

				got = false
				err = codec.Get(key, &got)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeTrue())
			})

			It("does not cache when Func fails", func() {
				perform(100, func(int) {
					var got bool
					err := codec.Once(&cache.Item{
						Key:    key,
						Object: &got,
						Func: func() (interface{}, error) {
							return nil, io.EOF
						},
					})
					Expect(err).To(Equal(io.EOF))
					Expect(got).To(BeFalse())
				})

				var got bool
				err := codec.Get(key, &got)
				Expect(err).To(Equal(cache.ErrCacheMiss))

				err = codec.Once(&cache.Item{
					Key:    key,
					Object: &got,
					Func: func() (interface{}, error) {
						return true, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeTrue())
			})

			It("works with Object", func() {
				var callCount int64
				perform(100, func(int) {
					got := new(Object)
					err := codec.Once(&cache.Item{
						Key:    key,
						Object: got,
						Func: func() (interface{}, error) {
							atomic.AddInt64(&callCount, 1)
							return obj, nil
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(got).To(Equal(obj))
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works with ptr and non-ptr", func() {
				var callCount int64
				perform(100, func(int) {
					got := new(Object)
					err := codec.Once(&cache.Item{
						Key:    key,
						Object: got,
						Func: func() (interface{}, error) {
							atomic.AddInt64(&callCount, 1)
							return *obj, nil
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(got).To(Equal(obj))
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works with bool", func() {
				var callCount int64
				perform(100, func(int) {
					var got bool
					err := codec.Once(&cache.Item{
						Key:    key,
						Object: &got,
						Func: func() (interface{}, error) {
							atomic.AddInt64(&callCount, 1)
							return true, nil
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(got).To(BeTrue())
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works without Object and nil result", func() {
				var callCount int64
				perform(100, func(int) {
					err := codec.Once(&cache.Item{
						Key: key,
						Func: func() (interface{}, error) {
							atomic.AddInt64(&callCount, 1)
							return nil, nil
						},
					})
					Expect(err).NotTo(HaveOccurred())
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works without Object and error result", func() {
				var callCount int64
				perform(100, func(int) {
					err := codec.Once(&cache.Item{
						Key: key,
						Func: func() (interface{}, error) {
							time.Sleep(100 * time.Millisecond)
							atomic.AddInt64(&callCount, 1)
							return nil, errors.New("error stub")
						},
					})
					Expect(err).To(MatchError("error stub"))
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("does not cache error result", func() {
				var callCount int64
				do := func(sleep time.Duration) (int, error) {
					var n int
					err := codec.Once(&cache.Item{
						Key:    key,
						Object: &n,
						Func: func() (interface{}, error) {
							time.Sleep(sleep)

							n := atomic.AddInt64(&callCount, 1)
							if n == 1 {
								return nil, errors.New("error stub")
							}
							return 42, nil
						},
					})
					if err != nil {
						return 0, err
					}
					return n, nil
				}

				perform(100, func(int) {
					n, err := do(100 * time.Millisecond)
					Expect(err).To(MatchError("error stub"))
					Expect(n).To(Equal(0))
				})

				perform(100, func(int) {
					n, err := do(0)
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(Equal(42))
				})

				Expect(callCount).To(Equal(int64(2)))
			})
		})
	}

	BeforeEach(func() {
		obj = &Object{
			Str: "mystring",
			Num: 42,
		}
	})

	Context("without LocalCache", func() {
		BeforeEach(func() {
			codec = newCodec()
		})

		testCodec()
	})

	Context("with LocalCache", func() {
		BeforeEach(func() {
			codec = newCodec()
			codec.UseLocalCache(1000, time.Minute)
		})

		testCodec()
	})

	Context("with LocalCache and without Redis", func() {
		BeforeEach(func() {
			codec = newCodec()
			codec.UseLocalCache(1000, time.Minute)
			codec.Redis = nil
		})

		testCodec()
	})
})

func newRing() *redis.Ring {
	return redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": ":6379",
			"server2": ":6380",
		},
	})
}

func newCodec() *cache.Codec {
	ring := newRing()
	_ = ring.ForEachShard(func(client *redis.Client) error {
		return client.FlushDb().Err()
	})

	return &cache.Codec{
		Redis: ring,

		Marshal: func(v interface{}) ([]byte, error) {
			return msgpack.Marshal(v)
		},
		Unmarshal: func(b []byte, v interface{}) error {
			return msgpack.Unmarshal(b, v)
		},
	}
}

func shuffle(slice interface{}) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rv := reflect.ValueOf(slice)
	swap := reflect.Swapper(slice)
	length := rv.Len()
	for i := length - 1; i > 0; i-- {
		j := rnd.Intn(i + 1)
		swap(i, j)
	}
}

