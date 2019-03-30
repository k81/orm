package orm

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
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
		return parseJSON([]byte(rawVal), jv.addr)
	case []byte:
		return parseJSON(rawVal, jv.addr)
	default:
		return errors.New("invalid type for json raw data")
	}
}

// getInnerPtrValue
// val: *struct{}
// ind: struct{}
func getInnerPtrValue(ptr interface{}) (val, ind reflect.Value) {
	val = reflect.ValueOf(ptr)
	ind = reflect.Indirect(val)

	for {
		switch ind.Kind() {
		case reflect.Interface:
			fallthrough
		case reflect.Ptr:
			if ind.IsNil() {
				ind.Set(reflect.New(ind.Type().Elem()))
			}
			val = ind
			ind = val.Elem()
		default:
			return val, ind
		}
	}
}

func parseJSON(data []byte, ptr interface{}) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil
	}

	val, ind := getInnerPtrValue(ptr)
	ptr = val.Interface()
	_, ok := ptr.(DynamicFielder)
	if !ok {
		return json.Unmarshal(data, ptr)
	}

	dynFieldMap := make(map[string]*json.RawMessage)
	typ := ind.Type()
	for i := 0; i < ind.NumField(); i++ {
		sf := typ.Field(i)
		field := ind.Field(i)

		if !field.CanSet() {
			continue
		}

		dynamic := sf.Tag.Get("dynamic")
		if dynamic == "true" {
			rawMsg := new(json.RawMessage)
			field.Set(reflect.ValueOf(rawMsg))
			dynFieldMap[sf.Name] = rawMsg
		}
	}

	if err := json.Unmarshal(data, ptr); err != nil {
		return err
	}

	for name, rawMsg := range dynFieldMap {
		field := ind.FieldByName(name)
		dynVal := ptr.(DynamicFielder).NewDynamicField(name)
		if dynVal != nil && len(*rawMsg) > 0 {
			if err := parseJSON([]byte(*rawMsg), dynVal); err != nil {
				return err
			}
			field.Set(reflect.ValueOf(dynVal))
		} else {
			field.Set(reflect.Zero(field.Type())) // for json:",omitempty"
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
