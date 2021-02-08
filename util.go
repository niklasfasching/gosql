package gosql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
)

type Connection interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type JSON struct{ Value interface{} }

var defaultFuncs = map[string]interface{}{
	"json_includes":  jsonIncludes,
	"regexp_extract": regexpExtract,
	"haversine":      haversine,
}

var regexpExtractRegexps = map[string]*regexp.Regexp{}

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
	add := func(k string, v interface{}) {
		ks = append(ks, k)
		qs = append(qs, "?")
		vs = append(vs, v)
	}
	switch rv.Kind() {
	case reflect.Map:
		m := rv.MapRange()
		for m.Next() {
			switch v := m.Value().Elem(); v.Kind() {
			case reflect.Map, reflect.Struct, reflect.Slice:
				bs, err := json.Marshal(v.Interface())
				if err != nil {
					return nil, err
				}
				add(m.Key().String(), string(bs))
			default:
				add(m.Key().String(), v.Interface())
			}
		}
	case reflect.Struct:
		for i, rt := 0, rv.Type(); i < rv.NumField(); i++ {
			add(rt.Field(i).Name, rv.Field(i).Interface())
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

func jsonIncludes(s string, v interface{}) (bool, error) {
	xs := []interface{}{}
	if err := json.Unmarshal([]byte(s), &xs); err != nil {
		return false, err
	}
	for _, x := range xs {
		if fmt.Sprintf("%v", x) == fmt.Sprintf("%v", v) {
			return true, nil
		}
	}
	return false, nil
}

func regexpExtract(input, regexpString string, i int) (string, error) {
	r, err := regexpExtractRegexps[regexpString], error(nil)
	if r == nil {
		r, err = regexp.Compile(regexpString)
		if err != nil {
			return "", err
		}
		regexpExtractRegexps[regexpString] = r
	}
	if m := r.FindStringSubmatch(input); len(m) > i {
		return m[i], nil
	}
	return "", nil
}

func haversine(latA, lngA, latB, lngB float64) float64 {
	latA = latA * math.Pi / 180
	lngA = lngA * math.Pi / 180
	latB = latB * math.Pi / 180
	lngB = lngB * math.Pi / 180
	dLat, dLng := latB-latA, lngB-lngA
	a := math.Pow(math.Sin(dLat/2), 2) + math.Cos(latA)*math.Cos(latB)*math.Pow(math.Sin(dLng/2), 2)
	c := 2 * math.Asin(math.Sqrt(a))
	return c * 6371 // earth radius km
}
