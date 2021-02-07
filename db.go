package gosql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"

	sqlite "github.com/mattn/go-sqlite3"
	sqlite3 "github.com/mattn/go-sqlite3"
)

type DB struct {
	DataSourceName string
	Funcs          map[string]interface{}
	RODB           *sql.DB
	*sql.DB
}

var driverIndex = 0

func (db *DB) Open(migrations map[string]string) error {
	if db.DB != nil {
		return errors.New("already open")
	}
	funcs := map[string]interface{}{}
	for k, v := range defaultFuncs {
		funcs[k] = v
	}
	for k, v := range db.Funcs {
		funcs[k] = v
	}
	db.Funcs = funcs
	rwDriver, roDriver := fmt.Sprintf("sqlite3-%d", driverIndex), fmt.Sprintf("sqlite3-read-only-%d", driverIndex)
	driverIndex++
	sql.Register(rwDriver, &sqlite3.SQLiteDriver{ConnectHook: db.connectHook})
	sql.Register(roDriver, &sqlite3.SQLiteDriver{ConnectHook: db.readOnlyConnectHook})
	if rwDB, err := sql.Open(rwDriver, db.DataSourceName); err != nil {
		return err
	} else {
		db.DB = rwDB
	}
	if roDB, err := sql.Open(roDriver, db.DataSourceName); err != nil {
		return err
	} else {
		db.RODB = roDB
	}
	return db.migrate(migrations)
}

func (db *DB) connectHook(c *sqlite.SQLiteConn) error {
	for name, f := range db.Funcs {
		if err := c.RegisterFunc(name, f, false); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) readOnlyConnectHook(c *sqlite.SQLiteConn) error {
	for name, f := range db.Funcs {
		if err := c.RegisterFunc(name, f, false); err != nil {
			return err
		}
	}
	c.RegisterAuthorizer(func(op int, arg1, arg2, arg3 string) int {
		switch op {
		case sqlite.SQLITE_SELECT, sqlite.SQLITE_READ, sqlite.SQLITE_FUNCTION:
			return sqlite.SQLITE_OK
		case sqlite.SQLITE_PRAGMA:
			switch arg1 {
			case "table_info", "data_version":
				return sqlite.SQLITE_OK
			case "user_version":
				if arg2 == "" && arg3 == "" {
					return sqlite.SQLITE_OK
				}
			}
		case sqlite.SQLITE_UPDATE: // necessary for fts5. see commit message
			if arg1 == "sqlite_master" && arg3 == "main" {
				return sqlite.SQLITE_OK
			}
		}
		return sqlite.SQLITE_DENY
	})
	return nil
}

func (db *DB) migrate(migrations map[string]string) error {
	q := "CREATE TABLE IF NOT EXISTS _migrations (name STRING, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
	if _, err := db.Exec(q); err != nil {
		return err
	}
	names, applied := []string{}, map[string]bool{}
	if err := Query(db, "SELECT name FROM _migrations", &names); err != nil {
		return err
	}
	for _, name := range names {
		applied[name] = true
	}
	keys := []string{}
	for key := range migrations {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if applied[key] {
			continue
		}
		if _, err := db.Exec(migrations[key]); err != nil {
			return err
		}
		if _, err := db.Exec("INSERT INTO _migrations (name) VALUES (?)", key); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Handler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	query, args, results := r.URL.Query().Get("query"), []interface{}{}, []map[string]JSON{}
	for _, arg := range r.URL.Query()["arg"] {
		args = append(args, arg)
	}
	if err := Query(db.RODB, query, &results, args...); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	} else {
		json.NewEncoder(w).Encode(results)
	}
}

func (db *DB) GetVersion() (int, error) {
	results := []int{}
	if err := Query(db, "PRAGMA user_version", &results); err != nil {
		return 0, err
	}
	return results[0], nil
}

func (db *DB) SetVersion(version int) error {
	_, err := Exec(db, fmt.Sprintf("PRAGMA user_version = %d", version))
	return err
}
