package cache_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/go-redis/redis/v7"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-redis/cache/v8"
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

var _ = Describe("Cache", func() {
	ctx := context.TODO()

	const key = "mykey"
	var obj *Object

	var mycache *cache.Cache

	testCache := func() {
		It("Gets and Sets nil", func() {
			err := mycache.Set(&cache.Item{
				Key: key,
				TTL: time.Hour,
			})
			Expect(err).NotTo(HaveOccurred())

			err = mycache.Get(ctx, key, nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(mycache.Exists(ctx, key)).To(BeTrue())
		})

		It("Deletes key", func() {
			err := mycache.Set(&cache.Item{
				Ctx: ctx,
				Key: key,
				TTL: time.Hour,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(mycache.Exists(ctx, key)).To(BeTrue())

			err = mycache.Delete(ctx, key)
			Expect(err).NotTo(HaveOccurred())

			err = mycache.Get(ctx, key, nil)
			Expect(err).To(Equal(cache.ErrCacheMiss))

			Expect(mycache.Exists(ctx, key)).To(BeFalse())
		})

		It("Gets and Sets data", func() {
			err := mycache.Set(&cache.Item{
				Ctx:   ctx,
				Key:   key,
				Value: obj,
				TTL:   time.Hour,
			})
			Expect(err).NotTo(HaveOccurred())

			wanted := new(Object)
			err = mycache.Get(ctx, key, wanted)
			Expect(err).NotTo(HaveOccurred())
			Expect(wanted).To(Equal(obj))

			Expect(mycache.Exists(ctx, key)).To(BeTrue())
		})

		It("Sets string as is", func() {
			value := "str_value"

			err := mycache.Set(&cache.Item{
				Ctx:   ctx,
				Key:   key,
				Value: value,
			})
			Expect(err).NotTo(HaveOccurred())

			var dst string
			err = mycache.Get(ctx, key, &dst)
			Expect(err).NotTo(HaveOccurred())
			Expect(dst).To(Equal(value))
		})

		It("Sets bytes as is", func() {
			value := []byte("str_value")

			err := mycache.Set(&cache.Item{
				Ctx:   ctx,
				Key:   key,
				Value: value,
			})
			Expect(err).NotTo(HaveOccurred())

			var dst []byte
			err = mycache.Get(ctx, key, &dst)
			Expect(err).NotTo(HaveOccurred())
			Expect(dst).To(Equal(value))
		})

		Describe("Once func", func() {
			It("calls Func when cache fails", func() {
				err := mycache.Set(&cache.Item{
					Ctx:   ctx,
					Key:   key,
					Value: int64(0),
				})
				Expect(err).NotTo(HaveOccurred())

				var got bool
				err = mycache.Get(ctx, key, &got)
				Expect(err).To(MatchError("msgpack: invalid code=0 decoding bool"))

				err = mycache.Once(&cache.Item{
					Ctx:   ctx,
					Key:   key,
					Value: &got,
					Do: func(*cache.Item) (interface{}, error) {
						return true, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeTrue())

				got = false
				err = mycache.Get(ctx, key, &got)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeTrue())
			})

			It("does not cache when Func fails", func() {
				perform(100, func(int) {
					var got bool
					err := mycache.Once(&cache.Item{
						Ctx:   ctx,
						Key:   key,
						Value: &got,
						Do: func(*cache.Item) (interface{}, error) {
							return nil, io.EOF
						},
					})
					Expect(err).To(Equal(io.EOF))
					Expect(got).To(BeFalse())
				})

				var got bool
				err := mycache.Get(ctx, key, &got)
				Expect(err).To(Equal(cache.ErrCacheMiss))

				err = mycache.Once(&cache.Item{
					Ctx:   ctx,
					Key:   key,
					Value: &got,
					Do: func(*cache.Item) (interface{}, error) {
						return true, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeTrue())
			})

			It("works with Value", func() {
				var callCount int64
				perform(100, func(int) {
					got := new(Object)
					err := mycache.Once(&cache.Item{
						Ctx:   ctx,
						Key:   key,
						Value: got,
						Do: func(*cache.Item) (interface{}, error) {
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
					err := mycache.Once(&cache.Item{
						Ctx:   ctx,
						Key:   key,
						Value: got,
						Do: func(*cache.Item) (interface{}, error) {
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
					err := mycache.Once(&cache.Item{
						Ctx:   ctx,
						Key:   key,
						Value: &got,
						Do: func(*cache.Item) (interface{}, error) {
							atomic.AddInt64(&callCount, 1)
							return true, nil
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(got).To(BeTrue())
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works without Value and nil result", func() {
				var callCount int64
				perform(100, func(int) {
					err := mycache.Once(&cache.Item{
						Ctx: ctx,
						Key: key,
						Do: func(*cache.Item) (interface{}, error) {
							atomic.AddInt64(&callCount, 1)
							return nil, nil
						},
					})
					Expect(err).NotTo(HaveOccurred())
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works without Value and error result", func() {
				var callCount int64
				perform(100, func(int) {
					err := mycache.Once(&cache.Item{
						Ctx: ctx,
						Key: key,
						Do: func(*cache.Item) (interface{}, error) {
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
					err := mycache.Once(&cache.Item{
						Ctx:   ctx,
						Key:   key,
						Value: &n,
						Do: func(*cache.Item) (interface{}, error) {
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
			mycache = newCache()
		})

		testCache()
	})

	Context("with LocalCache", func() {
		BeforeEach(func() {
			mycache = newCacheWithLocal()
		})

		testCache()
	})

	Context("with LocalCache and without Redis", func() {
		BeforeEach(func() {
			mycache = cache.New(&cache.Options{
				LocalCache: fastcache.New(1 << 20),
			})
		})

		testCache()
	})
})

func newRing() *redis.Ring {
	return redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": ":6379",
		},
	})
}

func newCache() *cache.Cache {
	ring := newRing()
	_ = ring.ForEachShard(func(client *redis.Client) error {
		return client.FlushDB().Err()
	})

	return cache.New(&cache.Options{
		Redis: ring,
	})
}

func newCacheWithLocal() *cache.Cache {
	ring := newRing()
	_ = ring.ForEachShard(func(client *redis.Client) error {
		return client.FlushDB().Err()
	})

	return cache.New(&cache.Options{
		Redis:      ring,
		LocalCache: fastcache.New(1 << 20),
	})
}
