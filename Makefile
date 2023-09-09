.PHONY: all fmt

all:
	go test ./...
	go test ./... -short -race
	go test ./... -run=NONE -bench=. -benchmem
	env GOOS=linux GOARCH=386 go test ./...
	golangci-lint run

fmt:
	gofumpt -w ./
	goimports -w  -local github.com/go-redis/cache ./
