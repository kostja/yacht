all:
	go mod vendor
	go build -o yacht yacht.go color.go
