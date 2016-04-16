all:
	go test gopkg.in/go-redis/cache.v4
	go test gopkg.in/go-redis/cache.v4 -short -race
