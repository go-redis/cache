package cache_test

import (
	"fmt"
	"testing"

	"github.com/go-redis/cache/v8"
)

func BenchmarkOnce(b *testing.B) {
	mycache := newCacheWithLocal()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var n int
			err := mycache.Once(&cache.Item{
				Key:   "bench-once",
				Value: &n,
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
