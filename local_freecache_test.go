package cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestFreecache_Get(t *testing.T) {
	localCache := NewFreecache(2097152, time.Minute)
	val, ok := localCache.Get("test")
	assert.False(t, ok)
	assert.Nil(t, val)

	localCache.Set("test", []byte("value"))

	val, ok = localCache.Get("test")
	assert.True(t, ok)
	assert.Equal(t, []byte("value"), val)
}
