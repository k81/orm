package orm

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
)

// JSONValue is the json wrapper
type JSONValue struct {
	addr      interface{}
	omitEmpty bool
}

// Value implements sql.Valuer interface
func (jv *JSONValue) Value() (driver.Value, error) {
	if jv.omitEmpty {
		if jv.addr == nil {
			return "", nil
		}

		ind := reflect.Indirect(reflect.ValueOf(jv.addr))
		if IsEmptyValue(ind) {
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
	var reader io.Reader

	switch rawVal := value.(type) {
	case string:
		reader = strings.NewReader(rawVal)
	case []byte:
		reader = bytes.NewReader(rawVal)
	default:
		return errors.New("invalid type for json raw data")
	}

	return parseJSON(reader, jv.addr)
}

func parseJSON(r io.Reader, ptr interface{}) error {
	_, ok := ptr.(DynamicFielder)
	if !ok {
		return json.NewDecoder(r).Decode(ptr)
	}

	val := reflect.ValueOf(ptr)
	ind := reflect.Indirect(val)
	dynFieldMap := make(map[string]*json.RawMessage)

	for i := 0; i < ind.NumField(); i++ {
		sf := ind.Type().Field(i)
		field := ind.Field(i)

		dynamic := sf.Tag.Get("dynamic")
		if dynamic == "true" {
			rawMsg := new(json.RawMessage)
			field.Set(reflect.ValueOf(rawMsg))
			dynFieldMap[sf.Name] = rawMsg
		}
	}

	if err := json.NewDecoder(r).Decode(ptr); err != nil {
		return err
	}

	for name, rawMsg := range dynFieldMap {
		field := ind.FieldByName(name)
		dynVal := ptr.(DynamicFielder).NewDynamicField(name)
		if dynVal != nil {
			if err := parseJSON(bytes.NewReader([]byte(*rawMsg)), dynVal); err != nil {
				return err
			}
			field.Set(reflect.ValueOf(dynVal))
		}
	}

	return nil
}

func getJSONValue(v interface{}, omitEmpty bool) interface{} {
	return &JSONValue{
		addr:      v,
		omitEmpty: omitEmpty,
	}
}
