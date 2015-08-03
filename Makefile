all:
	go test gopkg.in/go-redis/cache.v1 -cpu=1,2,4
	go test gopkg.in/go-redis/cache.v1 -short -race
