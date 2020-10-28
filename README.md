# Redis cache library for Golang

[![Build Status](https://travis-ci.org/go-redis/cache.svg)](https://travis-ci.org/go-redis/cache)
[![GoDoc](https://godoc.org/github.com/go-redis/cache?status.svg)](https://pkg.go.dev/github.com/go-redis/cache/v8?tab=doc)

> :heart: [**Uptrace.dev** - distributed traces, logs, and errors in one place](https://uptrace.dev)

## Installation

go-redis/cache requires a Go version with [Modules](https://github.com/golang/go/wiki/Modules) support and uses import versioning. So please make sure to initialize a Go module before installation:

```shell
go mod init github.com/my/repo
go get -u github.com/go-redis/cache/v8
```

## Quickstart

```go
package cache_test

import (
    "context"
    "fmt"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/go-redis/cache/v8"
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

    mycache := cache.New(&cache.Options{
        Redis:      ring,
        LocalCache: cache.NewTinyLFU(1000, time.Minute),
    })

    ctx := context.TODO()
    key := "mykey"
    obj := &Object{
        Str: "mystring",
        Num: 42,
    }

    if err := mycache.Set(&cache.Item{
        Ctx:   ctx,
        Key:   key,
        Value: obj,
        TTL:   time.Hour,
    }); err != nil {
        panic(err)
    }

    var wanted Object
    if err := mycache.Get(ctx, key, &wanted); err == nil {
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

    mycache := cache.New(&cache.Options{
        Redis:      ring,
        LocalCache: cache.NewTinyLFU(1000, time.Minute),
    })

    obj := new(Object)
    err := mycache.Once(&cache.Item{
        Key:   "mykey",
        Value: obj, // destination
        Do: func(*cache.Item) (interface{}, error) {
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
```
