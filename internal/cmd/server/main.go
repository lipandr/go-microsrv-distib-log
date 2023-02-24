package main

import (
	"log"

	"github.com/lipandr/go-microsrv-distib-log/internal/server"
)

func main() {
	// Create a new server
	srv := server.New(":8080")
	log.Fatal(srv.ListenAndServe())
}
