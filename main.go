package main

import (
	"flag"
	"log"

	"github.com/trusch/storaged/server"
	"github.com/trusch/storaged/storage"
)

var listenAddr = flag.String("listen", ":80", "listen address")
var backendURI = flag.String("backend", "bolt:///usr/share/storaged.boltdb", "storage backend address (leveldb://, bolt:// and mongodb:// are supported)")

func main() {
	flag.Parse()
	store, err := storage.NewMetaStorage(*backendURI)
	if err != nil {
		log.Fatal(err)
	}
	server := server.New(*listenAddr, store)
	log.Fatal(server.ListenAndServe())
}
