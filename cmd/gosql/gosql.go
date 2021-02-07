package main

import (
	"log"
	"os"
	"strings"

	"github.com/niklasfasching/gosql"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("gosql DB_FILE [QUERY]")
	}
	db := &gosql.DB{DataSourceName: os.Args[1]}
	if err := db.Open(nil); err != nil {
		log.Fatal(err)
	}
	if len(os.Args) == 2 {
		if err := gosql.REPL(db, "> "); err != nil {
			log.Fatal(err)
		}
	} else {
		if rows, err := db.Query(strings.Join(os.Args[2:], " ")); err != nil {
			log.Fatal(err)
		} else if err := gosql.Table(os.Stdout, rows); err != nil {
			log.Fatal(err)
		}
	}
}
