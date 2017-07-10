package cache_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/cache"
)

func BenchmarkOnce(b *testing.B) {
	codec := newCodec()
	codec.UseLocalCache(1000, time.Minute)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var n int
			err := codec.Once(&cache.Item{
				Key:    "bench-once",
				Object: &n,
				Func: func() (interface{}, error) {
					return 42, nil
				},
			})
			if err != nil {
				panic(err)
			}
			if n != 42 {
				panic(fmt.Sprintf("%d != 42", n))
			}
		}
	})
}
