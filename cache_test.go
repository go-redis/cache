package cache_test

import (
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v4"
	"gopkg.in/vmihailenco/msgpack.v2"

	"gopkg.in/go-redis/cache.v4"
	"gopkg.in/go-redis/cache.v4/lrucache"
)

func TestModels(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cache")
}

var _ = Describe("Codec", func() {
	const key = "mykey"
	var obj Object

	var ring *redis.Ring
	var codec *cache.Codec

	testCodec := func() {
		It("Gets and Sets data", func() {
			err := codec.Set(&cache.Item{
				Key:        key,
				Object:     obj,
				Expiration: time.Hour,
			})
			Expect(err).NotTo(HaveOccurred())

			var wanted Object
			err = codec.Get(key, &wanted)
			Expect(err).NotTo(HaveOccurred())
			Expect(wanted).To(Equal(obj))
		})

		It("supports cache func", func() {
			got, err := codec.Do(&cache.Item{
				Key:    key,
				Object: Object{},
				Func: func() (interface{}, error) {
					return obj, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(obj))
		})

		It("supports cache func that returns an error", func() {
			got, err := codec.Do(&cache.Item{
				Key: key,
				Func: func() (interface{}, error) {
					return nil, errors.New("error stub")
				},
			})
			Expect(err).To(MatchError("error stub"))
			Expect(got).To(BeNil())
		})

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
		ring.FlushDb()

		obj = Object{
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
