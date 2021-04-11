package main

import (
	"flag"
	"log"
	"strings"

	"github.com/niklasfasching/gosql"
)

var debug = flag.Bool("d", false, "print debug output (query plan & execution time)")

func main() {
	flag.Parse()
	args, debug := flag.Args(), *debug
	if len(args) < 2 {
		log.Fatal("gosql DB_FILE [QUERY]")
	}
	db := &gosql.DB{DataSourceName: args[0]}
	if err := db.Open(nil); err != nil {
		log.Fatal(err)
	}
	if err := gosql.Print(db, debug, strings.Join(args[1:], " ")); err != nil {
		log.Fatal(err)
	}
}
