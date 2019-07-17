package main

import "github.com/gocql/gocql"
import "log"

func main() {
    cluster := gocql.NewCluster("127.0.0.1")
    cluster.Keyspace = "example"
	session, err := cluster.CreateSession()
    if err != nil {
        log.Fatal(err)
    }
    session.Query("select * from lwt")
}
