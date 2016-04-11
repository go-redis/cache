# Redis cache library for Golang [![Build Status](https://travis-ci.org/go-redis/cache.svg?branch=v3)](https://travis-ci.org/go-redis/cache)

## Installation

    go get gopkg.in/go-redis/cache.v3

## Quickstart


```go
package cache_test

import (
	"fmt"
	"time"

	"gopkg.in/redis.v3"
	"gopkg.in/vmihailenco/msgpack.v2"

	"gopkg.in/go-redis/cache.v3"
)

type Object struct {
	Str string
	Num int
}

func Example_caching() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": ":6379",
			"server2": ":6380",
		},

		DialTimeout:  3 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})

	codec := &cache.Codec{
		Redis: ring,

		Marshal: func(v interface{}) ([]byte, error) {
			return msgpack.Marshal(v)
		},
		Unmarshal: func(b []byte, v interface{}) error {
			return msgpack.Unmarshal(b, v)
		},
	}

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
```
