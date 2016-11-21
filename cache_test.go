package cache_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/go-redis/cache.v5"
	"gopkg.in/go-redis/cache.v5/lrucache"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v5"
	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestModels(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cache")
}

func perform(n int, cbs ...func(int)) {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(cb func(int), i int) {
				defer GinkgoRecover()
				defer wg.Done()

				cb(i)
			}(cb, i)
		}
	}
	wg.Wait()
}

var _ = Describe("Codec", func() {
	const key = "mykey"
	var obj *Object

	var ring *redis.Ring
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
		})

		It("Deletes key", func() {
			err := codec.Set(&cache.Item{
				Key:        key,
				Expiration: time.Hour,
			})
			Expect(err).NotTo(HaveOccurred())

			err = codec.Delete(key)
			Expect(err).NotTo(HaveOccurred())

			err = codec.Get(key, nil)
			Expect(err).To(Equal(cache.ErrCacheMiss))
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
		})

		It("supports cache func", func() {
			var callCount int64
			perform(100, func(int) {
				got, err := codec.Do(&cache.Item{
					Key:    key,
					Object: new(Object),
					Func: func() (interface{}, error) {
						atomic.AddInt64(&callCount, 1)
						return obj, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(obj))
			})
			Expect(atomic.LoadInt64(&callCount)).To(Equal(int64(1)))
		})

		It("supports cache func without Object", func() {
			var callCount int64
			perform(100, func(int) {
				got, err := codec.Do(&cache.Item{
					Key: key,
					Func: func() (interface{}, error) {
						atomic.AddInt64(&callCount, 1)
						return true, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeTrue())
			})
			Expect(atomic.LoadInt64(&callCount)).To(Equal(int64(1)))
		})

		It("supports cache func without Object", func() {
			var callCount int64
			perform(100, func(int) {
				got, err := codec.Do(&cache.Item{
					Key: key,
					Func: func() (interface{}, error) {
						atomic.AddInt64(&callCount, 1)
						return nil, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeNil())
			})
			Expect(atomic.LoadInt64(&callCount)).To(Equal(int64(1)))
		})

		It("supports cache func that returns an error", func() {
			var callCount int64
			perform(100, func(int) {
				got, err := codec.Do(&cache.Item{
					Key: key,
					Func: func() (interface{}, error) {
						atomic.AddInt64(&callCount, 1)
						return nil, errors.New("error stub")
					},
				})
				Expect(err).To(MatchError("error stub"))
				Expect(got).To(BeNil())
			})
			Expect(atomic.LoadInt64(&callCount)).To(BeNumerically(">=", int64(1)))
		})
	}

	BeforeEach(func() {
		ring = redis.NewRing(&redis.RingOptions{
			Addrs: map[string]string{
				"server1": ":6379",
				"server2": ":6380",
			},

			DialTimeout:  3 * time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
		})
		_ = ring.ForEachShard(func(client *redis.Client) error {
			return client.FlushDb().Err()
		})

		obj = &Object{
			Str: "mystring",
			Num: 42,
		}
	})

	Context("without Cache", func() {
		BeforeEach(func() {
			codec = &cache.Codec{
				Redis: ring,

				Marshal: func(v interface{}) ([]byte, error) {
					return msgpack.Marshal(v)
				},
				Unmarshal: func(b []byte, v interface{}) error {
					return msgpack.Unmarshal(b, v)
				},
			}

			_ = ring.ForEachShard(func(client *redis.Client) error {
				return client.FlushDb().Err()
			})
		})

		testCodec()
	})

	Context("with Cache", func() {
		BeforeEach(func() {
			codec = &cache.Codec{
				Redis:      ring,
				LocalCache: lrucache.New(time.Minute, 1000),

				Marshal: func(v interface{}) ([]byte, error) {
					return msgpack.Marshal(v)
				},
				Unmarshal: func(b []byte, v interface{}) error {
					return msgpack.Unmarshal(b, v)
				},
			}
		})

		testCodec()
	})
})
