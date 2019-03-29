package cache_test

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/vmihailenco/msgpack"

	"../cache"
)

type Object struct {
	Str string
	Num int
}

func Example_basicUsage() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": ":6379",
			"server2": ":6380",
		},
	})

	codec := cache.NewCodec(ring,
		func(v interface{}) ([]byte, error) {
			return msgpack.Marshal(v)
		},
		func(b []byte, v interface{}) error {
			return msgpack.Unmarshal(b, v)
		})

	key := "mykey"
	obj := &Object{
		Str: "mystring",
		Num: 42,
	}

	codec.Set(&cache.Item{
		Key:        key,
		Object:     obj,
		Expiration: time.Hour,
	})

	var wanted Object
	if err := codec.Get(key, &wanted); err == nil {
		fmt.Println(wanted)
	}

	// Output: {mystring 42}
}

func Example_advancedUsage() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": ":6379",
			"server2": ":6380",
		},
	})

	codec := cache.NewCodec(ring,
		func(v interface{}) ([]byte, error) {
			return msgpack.Marshal(v)
		},
		func(b []byte, v interface{}) error {
			return msgpack.Unmarshal(b, v)
		})

	obj := new(Object)
	err := codec.Once(&cache.Item{
		Key:    "mykey",
		Object: obj, // destination
		Func: func() (interface{}, error) {
			return &Object{
				Str: "mystring",
				Num: 42,
			}, nil
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(obj)
	// Output: &{mystring 42}
}
