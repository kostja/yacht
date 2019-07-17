all:
	go mod vendor
	go build -o bin/yacht src/yacht.go
