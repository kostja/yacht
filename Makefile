all:
	go mod vendor
	go build -o yacht yacht.go color.go cql.go cql_connection.go cql_server.go
