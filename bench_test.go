package cache_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/cache"
)

func BenchmarkDo(b *testing.B) {
	codec := newCodec()
	codec.UseLocalCache(1000, time.Minute)

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
