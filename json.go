package orm

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/k81/dynamicjson"
)

// JSONValue is the json wrapper
type JSONValue struct {
	addr      interface{}
	omitEmpty bool
}

func newJSONValue(v interface{}, omitEmpty bool) interface{} {
	return &JSONValue{
		addr:      v,
		omitEmpty: omitEmpty,
	}
}

// Value implements sql.Valuer interface
func (jv *JSONValue) Value() (driver.Value, error) {
	if jv.omitEmpty {
		if jv.addr == nil {
			return "", nil
		}

		if IsEmptyValue(reflect.ValueOf(jv.addr)) {
			return "", nil
		}
	}

	data, err := json.Marshal(jv.addr)
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

// Scan implements sql.Scanner interface
func (jv *JSONValue) Scan(value interface{}) error {
	switch rawVal := value.(type) {
	case string:
		return dynamicjson.Parse([]byte(rawVal), jv.addr)
	case []byte:
		return dynamicjson.Parse(rawVal, jv.addr)
	default:
		return errors.New("invalid type for json raw data")
	}
}
