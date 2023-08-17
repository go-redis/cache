package cache_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-redis/cache/v9"
)

func TestTinyLFU_Get_CorruptionOnExpiry(t *testing.T) {
	strFor := func(i int) string {
		return fmt.Sprintf("a string %d", i)
	}
	keyName := func(i int) string {
		return fmt.Sprintf("key-%00000d", i)
	}

	mycache := cache.NewTinyLFU(1000, 1*time.Second)
	size := 50000
	// Put a bunch of stuff in the cache with a TTL of 1 second
	for i := 0; i < size; i++ {
		key := keyName(i)
		mycache.Set(key, []byte(strFor(i)))
	}

	// Read stuff for a bit longer than the TTL - that's when the corruption occurs
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := ctx.Done()
loop:
	for {
		select {
		case <-done:
			// this is expected
			break loop
		default:
			i := rand.Intn(size)
			key := keyName(i)

			b, ok := mycache.Get(key)
			if !ok {
				continue loop
			}

			got := string(b)
			expected := strFor(i)
			if got != expected {
				t.Fatalf("expected=%q got=%q key=%q", expected, got, key)
			}
		}
	}
}

func TestTinyLFU_Clear(t *testing.T) {
	rounds := 100
	makeKey := func(round int) string {
		return fmt.Sprintf("key-%d", round)
	}
	makeVal := func(round int) []byte {
		return []byte(fmt.Sprintf("%d", round))
	}

	myCache := cache.NewTinyLFU(rounds+1, time.Hour)
	for i := 0; i < rounds; i++ {
		myCache.Set(makeKey(i), makeVal(i))
	}

	// ensure entries are present
	for i := 0; i < rounds; i++ {
		key := makeKey(i)
		expected := string(makeVal(i))
		b, ok := myCache.Get(key)
		if !ok {
			t.Fatalf("expected key=%q to be present (expected=%q), but was missing", key, expected)
		}
		got := string(b)
		if expected != got {
			t.Fatalf("expected=%q, got=%q, key=%q", expected, got, key)
		}
	}

	myCache.Clear()

	for i := 0; i < rounds; i++ {
		key := makeKey(i)
		b, ok := myCache.Get(key)
		if ok {
			got := string(b)
			t.Fatalf("expected key=%q to be missing, got=%s", key, got)
		}
	}
}
