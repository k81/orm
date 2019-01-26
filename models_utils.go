package orm

import (
	"context"
	"reflect"
	"strings"
)

const (
	TagTypeNoArgs       = 1
	TagTypeWithArgs     = 2
	TagTypeOptionalArgs = 3
)

// 1 is attr
// 2 is tag
var supportTag = map[string]int{
	"-":      TagTypeNoArgs,
	"pk":     TagTypeNoArgs,
	"auto":   TagTypeNoArgs,
	"json":   TagTypeOptionalArgs,
	"column": TagTypeWithArgs,
}

// get reflect.Type name with package path.
func getFullName(typ reflect.Type) string {
	return typ.PkgPath() + "." + typ.Name()
}

// getTableName get struct table name.
// If the struct implement the TableName, then get the result as tablename
// else use the struct name which will apply snakeString.
func getTableName(val reflect.Value) string {
	if fun := val.MethodByName("TableName"); fun.IsValid() {
		vals := fun.Call([]reflect.Value{})
		// has return and the first val is string
		if len(vals) > 0 && vals[0].Kind() == reflect.String {
			return vals[0].String()
		}
	}
	return snakeString(reflect.Indirect(val).Type().Name())
}

func isSharded(val reflect.Value) bool {
	if fun := val.MethodByName("TableSuffix"); fun.IsValid() {
		return true
	}
	return false
}

func getTableSuffix(val reflect.Value) string {
	if fun := val.MethodByName("TableSuffix"); fun.IsValid() {
		vals := fun.Call([]reflect.Value{})
		if len(vals) > 0 && vals[0].Kind() == reflect.String {
			return vals[0].String()
		}
	}
	return ""
}

// get snaked column name
func getColumnName(sf reflect.StructField, col string) string {
	column := col
	if col == "" {
		column = snakeString(sf.Name)
	}
	return column
}

// parse struct tag string
func parseStructTag(data string) (attrs map[string]bool, tags map[string]string) {
	attrs = make(map[string]bool)
	tags = make(map[string]string)
	for _, v := range strings.Split(data, defaultStructTagDelim) {
		if v == "" {
			continue
		}
		v = strings.TrimSpace(v)
		var (
			tag  string
			args string
		)

		i := strings.Index(v, "(")
		switch {
		case i < 0:
			tag = v
		case i > 0 && strings.Index(v, ")") == (len(v)-1):
			tag = v[:i]
			args = v[i+1 : len(v)-1]
		}

		tagTyp, ok := supportTag[tag]
		if !ok {
			logger.Error(context.TODO(), "unsupport orm tag", "tag", v)
			return
		}

		switch tagTyp {
		case TagTypeNoArgs:
			if args != "" {
				logger.Error(context.TODO(), "tag not support argument", "tag", tag)
				return
			}
			attrs[tag] = true
		case TagTypeWithArgs:
			if args == "" {
				logger.Error(context.TODO(), "tag missing argument", "tag", tag)
				return
			}
			tags[tag] = args
		case TagTypeOptionalArgs:
			attrs[tag] = true
			if args != "" {
				tags[tag] = args
			}
		}
	}
	return
}
