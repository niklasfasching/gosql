package gosql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
)

type DB struct {
	MigrationsTable string
	MigrationsDir   string
	DriverName      string
	DataSourceName  string
	*sql.DB
}

func (db *DB) Open() error {
	if db.DB != nil {
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
	db.DB = connection
	return db.migrate()
}

func (db *DB) Close() error { return db.DB.Close() }

func (db *DB) migrate() error {
	t := db.MigrationsTable
	q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (name STRING, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", t)
	if _, err := db.DB.Exec(q); err != nil {
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
		if _, err = db.DB.Exec(string(bs)); err != nil {
			return err
		}
		if _, err := db.DB.Exec(fmt.Sprintf("INSERT INTO `%s` (name) VALUES (?)", t), sqlFile); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Query(query string, result interface{}, args ...interface{}) error {
	xs := reflect.ValueOf(result)
	if result != nil && (xs.Type().Kind() != reflect.Ptr || xs.Type().Elem().Kind() != reflect.Slice) {
		return fmt.Errorf("cannot unmarshal query results into %t (%v)", result, result)
	}
	if result == nil {
		_, err := db.DB.Exec(query, args...)
		return err
	}
	rows, err := db.DB.Query(query, args...)
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
	switch dst.(type) {
	case *interface{}:
		if s := string(bs); len(s) >= 4 && s[0] == '"' && s[len(s)-1] == '"' &&
			(s[1] == '{' && s[len(s)-2] == '}' || s[1] == '[' && s[len(s)-2] == ']') {
			if s, err := strconv.Unquote(s); err == nil {
				bs = []byte(s)
			}
		}
	}
	return json.Unmarshal(bs, &dst)
}
