all:
	go mod vendor
	go build -mod=vendor -o yacht yacht.go color.go cql.go cql_connection.go cql_server.go
