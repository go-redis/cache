all:
	go test gopkg.in/go-redis/cache.v3 -cpu=1,2,4
	go test gopkg.in/go-redis/cache.v3 -short -race
