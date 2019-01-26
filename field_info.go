package orm

import (
	"errors"
	"fmt"
	"reflect"
)

var errSkipField = errors.New("skip field")

// single field info
type fieldInfo struct {
	mi            *modelInfo
	fieldIndex    []int
	name          string
	fullName      string
	column        string
	addrValue     reflect.Value
	sf            reflect.StructField
	pk            bool
	auto          bool
	json          bool
	jsonOmitEmpty bool
	dynamic       bool
}

// new field info
func newFieldInfo(mi *modelInfo, field reflect.Value, sf reflect.StructField, mName string) (fi *fieldInfo, err error) {
	var (
		attrs     map[string]bool
		tags      map[string]string
		addrField reflect.Value
	)

	fi = new(fieldInfo)

	// if field which CanAddr is the follow type
	//  A value is addressable if it is an element of a slice,
	//  an element of an addressable array, a field of an
	//  addressable struct, or the result of dereferencing a pointer.
	addrField = field
	if field.CanAddr() && field.Kind() != reflect.Ptr {
		addrField = field.Addr()
	}

	attrs, tags = parseStructTag(sf.Tag.Get(defaultStructTagName))
	if _, ok := attrs["-"]; ok {
		return nil, errSkipField
	}

	fi.name = sf.Name
	fi.column = getColumnName(sf, tags["column"])
	fi.addrValue = addrField
	fi.sf = sf
	fi.fullName = mi.fullName + mName + "." + sf.Name
	fi.pk = attrs["pk"]
	fi.auto = attrs["auto"]
	fi.json = attrs["json"]
	if tags["json"] == "omitempty" {
		fi.jsonOmitEmpty = true
	}

	if fi.json {
		fi.dynamic = sf.Tag.Get("dynamic") == "true"
		if fi.dynamic {
			if field.Kind() != reflect.Interface {
				panic(fmt.Errorf("json dynamic field must be interface{}: %v", fi.fullName))
			}

			_, ok := mi.addrField.Interface().(DynamicFielder)
			if !ok {
				panic(fmt.Errorf("model must implement DynamicFielder interface: %v", mi.fullName))
			}
		} else {
			if field.Kind() == reflect.Ptr {
				panic(fmt.Errorf("json field must not be ptr: %v", fi.fullName))
			}
		}
	}

	return fi, nil
}
