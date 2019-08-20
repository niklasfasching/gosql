package gosql

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestUnmarshal(t *testing.T) {
	tmp, m := map[string]JSON{}, map[string]interface{}{}
	if err := convert(map[string]string{"array": "[1]", "map": "{\"foo\": 1}"}, &tmp); err != nil {
		t.Error(err)
		return
	}
	bs, err := json.Marshal(tmp)
	if err != nil {
		t.Error(err)
		return
	}
	if err := json.Unmarshal(bs, &m); err != nil {
		t.Error(err)
		return
	}
	expected := map[string]interface{}{
		"array": []interface{}{1.0},
		"map":   map[string]interface{}{"foo": 1.0},
	}
	if !reflect.DeepEqual(expected, m) {
		t.Errorf("%#v not %#v", m, expected)
	}
}
