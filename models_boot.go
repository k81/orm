package orm

import (
	"errors"
	"fmt"
	"reflect"
)

// register models.
// PrefixOrSuffix means table name prefix or suffix.
// isPrefix whether the prefix is prefix or suffix
func registerModel(PrefixOrSuffix, dbName string, model interface{}, isPrefix bool) {
	val := reflect.ValueOf(model)
	typ := reflect.Indirect(val).Type()

	if val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("register model: cannot use non-ptr model struct `%s`", getFullName(typ)))
	}
	// For this case:
	// u := &User{}
	// registerModel(&u)
	if typ.Kind() == reflect.Ptr {
		panic(errors.New("register model: only allow ptr model struct"))
	}

	table := getTableName(val)

	if PrefixOrSuffix != "" {
		if isPrefix {
			table = PrefixOrSuffix + table
		} else {
			table = table + PrefixOrSuffix
		}
	}
	// models's fullname is pkgpath + struct name
	fullName := getFullName(typ)
	if _, ok := modelCache.get(fullName); ok {
		panic(fmt.Errorf("register model: model `%s` repeat register, must be unique", fullName))
	}

	mi := newModelInfo(val)
	if mi.fields.pk == nil {
		panic(fmt.Errorf("register model: `%s` need a primary key field", fullName))
	}

	mi.db = dbName
	mi.table = table
	mi.pkg = typ.PkgPath()
	mi.model = model
	mi.sharded = isSharded(val)

	modelCache.add(mi)
}

// boostrap models
func bootStrap() {
	if modelCache.done {
		return
	}

	if dbCache.getDefault() == nil {
		panic(fmt.Errorf("must have one register DataBase alias named `default`"))
	}
}

// RegisterModel register models
func RegisterModel(db string, models ...interface{}) {
	if modelCache.done {
		panic(fmt.Errorf("RegisterModel must be run before BootStrap"))
	}
	RegisterModelWithPrefix("", db, models...)
}

// RegisterModelWithPrefix register models with a prefix
func RegisterModelWithPrefix(prefix, db string, models ...interface{}) {
	if modelCache.done {
		panic(fmt.Errorf("RegisterModelWithPrefix must be run before BootStrap"))
	}

	for _, model := range models {
		registerModel(prefix, db, model, true)
	}
}

// RegisterModelWithSuffix register models with a suffix
func RegisterModelWithSuffix(suffix, db string, models ...interface{}) {
	if modelCache.done {
		panic(fmt.Errorf("RegisterModelWithSuffix must be run before BootStrap"))
	}

	for _, model := range models {
		registerModel(suffix, db, model, false)
	}
}

// BootStrap bootrap models.
// make all model parsed and can not add more models
func BootStrap() {
	if modelCache.done {
		return
	}
	modelCache.Lock()
	defer modelCache.Unlock()
	bootStrap()
	modelCache.done = true
}
