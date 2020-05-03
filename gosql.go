package gosql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	sqlite "github.com/mattn/go-sqlite3"
	sqlite3 "github.com/mattn/go-sqlite3"
)

type DB struct {
	DataSourceName string
	readOnly       bool
	Funcs          map[string]interface{}
	*sql.DB
}

type Connection interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type JSON struct{ Value interface{} }

var driverIndex = 0

func (db *DB) Open(readOnly bool, migrations map[string]string) error {
	db.migrate(migrations)
	db.readOnly = readOnly
	if db.DB != nil {
		return errors.New("already open")
	}
	if err := db.migrate(migrations); err != nil {
		return err
	}
	driverName := fmt.Sprintf("sqlite3-%d", driverIndex)
	driverIndex++
	sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: db.connectHook})
	sqlDB, err := sql.Open(driverName, db.DataSourceName)
	if err != nil {
		return err
	}
	db.DB = sqlDB
	return nil
}

func (db *DB) connectHook(c *sqlite.SQLiteConn) error {
	for name, f := range db.Funcs {
		if err := c.RegisterFunc(name, f, false); err != nil {
			return err
		}
	}
	if db.readOnly {
		c.RegisterAuthorizer(func(op int, arg1, arg2, arg3 string) int {
			switch op {
			case sqlite.SQLITE_SELECT, sqlite.SQLITE_READ, sqlite.SQLITE_FUNCTION:
				return sqlite.SQLITE_OK
			case sqlite.SQLITE_PRAGMA:
				switch arg1 {
				case "table_info":
					return sqlite.SQLITE_OK
				}
			}
			return sqlite.SQLITE_DENY
		})
	}
	return nil
}

func (db *DB) migrate(migrations map[string]string) error {
	sqlDB, err := sql.Open("sqlite3", db.DataSourceName)
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	q := "CREATE TABLE IF NOT EXISTS _migrations (name STRING, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
	if _, err := sqlDB.Exec(q); err != nil {
		return err
	}
	names, applied := []string{}, map[string]bool{}
	if err := Query(sqlDB, "SELECT name FROM _migrations", &names); err != nil {
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
		if _, err = sqlDB.Exec(migrations[key]); err != nil {
			return err
		}
		if _, err := sqlDB.Exec("INSERT INTO _migrations (name) VALUES (?)", key); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Handler(params ...string) http.Handler {
	if !db.readOnly {
		panic(errors.New("must not serve a writable db - set ReadOnly to true"))
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		if len(params) == 0 {
			params = []string{"query", "q"}
		}
		query, result := "", []map[string]JSON{}
		for i := 0; i < len(params) && query == ""; i++ {
			query = r.URL.Query().Get(params[i])
		}
		if query == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "query must not be empty"})
			return
		}
		if err := Query(db, query, &result); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(result)
	})
}

func Query(c Connection, queryString string, result interface{}, args ...interface{}) error {
	if err := query(c, queryString, result, args...); err != nil {
		return fmt.Errorf("%s: %s", queryString, err)
	}
	return nil
}

func Exec(c Connection, queryString string, args ...interface{}) (sql.Result, error) {
	result, err := c.Exec(queryString, args...)
	if err != nil {
		err = fmt.Errorf("%s: %s", queryString, err)
	}
	return result, err
}

func Insert(c Connection, table string, v interface{}, ignore bool) (sql.Result, error) {
	rv, ks, qs, vs := reflect.ValueOf(v), []string{}, []string{}, []interface{}{}
	switch rv.Kind() {
	case reflect.Map:
		m := rv.MapRange()
		for m.Next() {
			ks = append(ks, m.Key().String())
			qs = append(qs, "?")
			switch v := m.Value().Elem(); v.Kind() {
			case reflect.Map, reflect.Struct, reflect.Slice:
				bs, err := json.Marshal(v.Interface())
				if err != nil {
					return nil, err
				}
				vs = append(vs, string(bs))
			default:
				vs = append(vs, v.Interface())
			}
		}
	default:
		return nil, fmt.Errorf("unhandled type %T", v)
	}
	maybeIgnore := ""
	if ignore {
		maybeIgnore = "OR IGNORE"
	}
	query := fmt.Sprintf("INSERT %s INTO %s (%s) VALUES (%s)", maybeIgnore, table, strings.Join(ks, ", "), strings.Join(qs, ", "))
	return c.Exec(query, vs...)
}

func query(c Connection, query string, result interface{}, args ...interface{}) error {
	xs := reflect.ValueOf(result)
	if xs.Kind() != reflect.Ptr || xs.Type().Elem().Kind() != reflect.Slice {
		return fmt.Errorf("cannot unmarshal query results into %t (%v)", result, result)
	}
	rows, err := c.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if err := unmarshal(rows, xs.Elem()); err != nil {
		return err
	}
	return rows.Err()
}

func unmarshal(rows *sql.Rows, xs reflect.Value) error {
	t, isPtr := xs.Type().Elem(), false
	switch t.Kind() {
	case reflect.Ptr:
		t, isPtr = t.Elem(), true
		fallthrough
	case reflect.Struct:
		return unmarshalStruct(rows, xs, t, isPtr)
	case reflect.Interface:
		t = reflect.TypeOf(map[string]interface{}{})
		fallthrough
	case reflect.Map:
		return unmarshalMap(rows, xs, t, isPtr)
	default:
		for rows.Next() {
			x := reflect.New(t)
			if err := scan(rows, []interface{}{x.Interface()}); err != nil {
				return err
			}
			if !isPtr {
				x = x.Elem()
			}
			xs.Set(reflect.Append(xs, x))
		}
	}
	return nil
}

func unmarshalStruct(rows *sql.Rows, xs reflect.Value, t reflect.Type, isPtr bool) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		x := reflect.New(t).Elem()
		values := []interface{}{}
		for _, column := range columns {
			field := x.FieldByName(column) // TODO: use tags / case conversion for struct -> sql field mapping
			if field.IsValid() {
				values = append(values, field.Addr().Interface())
			} else {
				values = append(values, new(interface{}))
			}
		}
		if err = scan(rows, values); err != nil {
			return err
		}
		if isPtr {
			x = x.Addr()
		}
		xs.Set(reflect.Append(xs, x))
	}
	return nil
}

func unmarshalMap(rows *sql.Rows, xs reflect.Value, t reflect.Type, isPtr bool) error {
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
		if err = scan(rows, values); err != nil {
			return err
		}
		x := reflect.MakeMapWithSize(mt, len(columns))
		for i, column := range columns {
			x.SetMapIndex(reflect.ValueOf(column), reflect.ValueOf(values[i]).Elem())
		}
		if isPtr {
			x = x.Addr()
		}
		xs.Set(reflect.Append(xs, x))
	}
	return nil
}

func scan(rows *sql.Rows, values []interface{}) error {
	tmp := make([]interface{}, len(values))
	for i := range values {
		tmp[i] = new(interface{})
	}
	if err := rows.Scan(tmp...); err != nil {
		return err
	}
	for i := range values {
		if err := convert(tmp[i], values[i]); err != nil {
			return err
		}
	}

	return nil
}

func convert(src, dst interface{}) error {
	bs, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(bs, dst)
}

func (j JSON) MarshalJSON() ([]byte, error) {
	switch s, ok := j.Value.(string); {
	case ok && isJSONArrayString(s):
		xs := []JSON{}
		if err := json.Unmarshal([]byte(s), &xs); err != nil {
			break
		}
		return json.Marshal(xs)
	case ok && isJSONObjectString(s):
		xs := map[string]JSON{}
		if err := json.Unmarshal([]byte(s), &xs); err != nil {
			break
		}
		return json.Marshal(xs)
	}
	return json.Marshal(j.Value)
}

func (j *JSON) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &j.Value)
}

func isJSONObjectString(s string) bool {
	return len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}'
}

func isJSONArrayString(s string) bool {
	return len(s) >= 2 && s[0] == '[' && s[len(s)-1] == ']'
}

func ReadMigrations(directory string) (map[string]string, error) {
	m := map[string]string{}
	sqlFiles, err := filepath.Glob(filepath.Join(directory, "*.sql"))
	if err != nil {
		return nil, err
	}
	for _, sqlFile := range sqlFiles {
		bs, err := ioutil.ReadFile(sqlFile)
		if err != nil {
			return nil, err
		}
		m[sqlFile] = string(bs)
	}
	return m, nil
}
