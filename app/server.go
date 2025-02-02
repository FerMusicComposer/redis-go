package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
)

var (
	storage sync.Map // Thread-safe concurrent map for key-value storage
)

func main() {
	dir := flag.String("dir", ".", "RDB file directory")
	dbFilename := flag.String("dbfilename", "dump.rdb", "RDB filename")
	flag.Parse()

	initConfig(*dir, *dbFilename)

	if err := loadRDBFile(); err != nil {
		fmt.Println("Error loading RDB file:", err)
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn)
	}
}
