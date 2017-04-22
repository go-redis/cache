package cache_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/cache"
	"github.com/go-redis/cache/lrucache"
)

func BenchmarkDo(b *testing.B) {
	codec := newCodec()
	codec.LocalCache = lrucache.New(time.Minute, 1000)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n, err := codec.Once(&cache.Item{
				Key: "bench-once",
				Func: func() (interface{}, error) {
					return uint64(42), nil
				},
			})
			if err != nil {
				panic(err)
			}
			if n.(uint64) != 42 {
				panic(fmt.Sprintf("%d != 42", n))
			}
		}
	})
}
