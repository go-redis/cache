all:
	go test ./
	go test ./ -short -race
	env GOOS=linux GOARCH=386 go test ./
	go vet
