package main

var config = struct {
	dir        string
	dbFilename string
}{
	dir:        ".",
	dbFilename: "dump.rdb",
}

func initConfig(dir, filename string) {
	config.dir = dir
	config.dbFilename = filename
}
