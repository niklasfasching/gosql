package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	MigrationsTable string
	MigrationsDir   string
	DriverName      string
	DataSourceName  string
	connection      *sql.DB
}

func main() {
	db := &DB{
		DriverName:     "sqlite3",
		DataSourceName: "./data.db",
	}
	if err := db.Open(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	http.Handle("/query", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		if query == "" {
			http.Error(w, "empty query", http.StatusBadRequest)
			return
		}
		results := []interface{}{}
		if err := db.Query(query, &results); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(results)
	}))
	log.Fatal(http.ListenAndServe(":8000", nil))

}

func (db *DB) Open() error {
	if db.connection != nil {
		return errors.New("already open")
	}
	if db.MigrationsTable == "" {
		db.MigrationsTable = "migrations"
	}
	if db.MigrationsDir == "" {
		db.MigrationsDir = "migrations"
	}
	connection, err := sql.Open(db.DriverName, db.DataSourceName)
	if err != nil {
		return err
	}
	db.connection = connection
	return db.migrate()
}

func (db *DB) Close() error { return db.connection.Close() }

func (db *DB) migrate() error {
	t := db.MigrationsTable
	q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (name STRING, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", t)
	if _, err := db.connection.Exec(q); err != nil {
		return err
	}
	applied, m := []string{}, map[string]bool{}
	if err := db.Query(fmt.Sprintf("SELECT name FROM `%s`", t), &applied); err != nil {
		return err
	}
	for _, sqlFile := range applied {
		m[sqlFile] = true
	}
	sqlFiles, err := filepath.Glob(filepath.Join(db.MigrationsDir, "*.sql"))
	if err != nil {
		return err
	}
	sort.Strings(sqlFiles)
	for _, sqlFile := range sqlFiles {
		if m[sqlFile] {
			continue
		}
		bs, err := ioutil.ReadFile(sqlFile)
		if err != nil {
			return err
		}
		if _, err = db.connection.Exec(string(bs)); err != nil {
			return err
		}
		if _, err := db.connection.Exec(fmt.Sprintf("INSERT INTO `%s` (name) VALUES (?)", t), sqlFile); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Query(query string, result interface{}, args ...interface{}) error {
	xs := reflect.ValueOf(result)
	if t := xs.Type(); result == nil || t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("cannot unmarshal query results into %t (%v)", result, result)
	}
	xs = xs.Elem()
	rows, err := db.connection.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	switch t := xs.Type().Elem(); t.Kind() {
	case reflect.Interface:
		t = reflect.TypeOf(map[string]interface{}{})
		fallthrough
	case reflect.Map:
		columns, err := rows.Columns()
		if err != nil {
			return err
		}
		mt := reflect.MapOf(reflect.TypeOf(""), t.Elem())
		values := []interface{}{}
		for range columns {
			values = append(values, reflect.New(t.Elem()).Interface())
		}
		for rows.Next() {
			if err = rows.Scan(values...); err != nil {
				return err
			}
			x := reflect.MakeMapWithSize(mt, len(columns))
			for i, column := range columns {
				x.SetMapIndex(reflect.ValueOf(column), reflect.ValueOf(values[i]).Elem())
			}
			xs.Set(reflect.Append(xs, x))
		}
	default:
		for rows.Next() {
			x := reflect.New(t)
			if err = rows.Scan(x.Interface()); err != nil {
				return err
			}
			xs.Set(reflect.Append(xs, x.Elem()))
		}
	}
	return rows.Err()
}
