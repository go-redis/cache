all:
	go test ./...
	go test ./... -short -race

