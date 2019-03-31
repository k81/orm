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
		return ParseJSON([]byte(rawVal), jv.addr)
	case []byte:
		return ParseJSON(rawVal, jv.addr)
	default:
		return errors.New("invalid type for json raw data")
	}
}

type dynField struct {
	Name  string
	Value *reflect.Value
}

// getInnerPtrValue
// val: *struct{}
// ind: struct{}
func getInnerPtrValue(ptr interface{}) (val, ind reflect.Value) {
	val = reflect.ValueOf(ptr)
	ind = reflect.Indirect(val)

	for {
		switch ind.Kind() {
		case reflect.Interface, reflect.Ptr:
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

func gatherDynFields(val reflect.Value, pFields *[]*dynField) {
	switch val.Kind() {
	case reflect.Interface, reflect.Ptr:
		if val.IsNil() {
			return
		}
		gatherDynFields(val.Elem(), pFields)
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			gatherDynFields(val.Index(i), pFields)
		}
	case reflect.Map:
		iter := val.MapRange()
		for iter.Next() {
			gatherDynFields(iter.Value(), pFields)
		}
	case reflect.Struct:
		typ := val.Type()
		for i := 0; i < val.NumField(); i++ {
			sf := typ.Field(i)
			field := val.Field(i)

			if !field.CanSet() {
				continue
			}

			dynamic := sf.Tag.Get("dynamic")
			if dynamic == "true" {
				rawMsg := new(json.RawMessage)
				field.Set(reflect.ValueOf(rawMsg))
				*pFields = append(*pFields, &dynField{
					Name:  sf.Name,
					Value: &field,
				})
			} else {
				gatherDynFields(field, pFields)
			}
		}
	}
}

// ParseJSON parse json with dynamic field parse support
func ParseJSON(data []byte, ptr interface{}) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		// ignore empty field
		return nil
	}

	val, ind := getInnerPtrValue(ptr)
	ptr = val.Interface()
	dynFields := []*dynField{}

	gatherDynFields(ind, &dynFields)

	if err := json.Unmarshal(data, ptr); err != nil {
		return err
	}

	for _, dynField := range dynFields {
		rawMsg := dynField.Value.Interface().(*json.RawMessage)
		dynVal := ptr.(DynamicFielder).NewDynamicField(dynField.Name)
		if dynVal != nil && len(*rawMsg) > 0 {
			if err := ParseJSON([]byte(*rawMsg), dynVal); err != nil {
				return err
			}
			dynField.Value.Set(reflect.ValueOf(dynVal))
		} else {
			dynField.Value.Set(reflect.Zero(dynField.Value.Type())) // for json:",omitempty"
		}
	}

	return nil
}
