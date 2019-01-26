package orm

import "strings"

// field info collection
type fields struct {
	pk        *fieldInfo
	auto      *fieldInfo
	columns   map[string]*fieldInfo
	fields    map[string]*fieldInfo
	fieldsLow map[string]*fieldInfo
	fieldsDB  []*fieldInfo
	orders    []string
	dbcols    []string
}

// Add add fieldInfo to fields
func (f *fields) Add(fi *fieldInfo) (added bool) {
	if f.fields[fi.name] == nil && f.columns[fi.column] == nil {
		f.columns[fi.column] = fi
		f.fields[fi.name] = fi
		f.fieldsLow[strings.ToLower(fi.name)] = fi
	} else {
		return
	}
	f.orders = append(f.orders, fi.column)
	f.dbcols = append(f.dbcols, fi.column)
	f.fieldsDB = append(f.fieldsDB, fi)
	return true
}

// get field info by name
func (f *fields) GetByName(name string) *fieldInfo {
	return f.fields[name]
}

// get field info by column name
func (f *fields) GetByColumn(column string) *fieldInfo {
	return f.columns[column]
}

// get field info by string, name is prior
func (f *fields) GetByAny(name string) (*fieldInfo, bool) {
	if fi, ok := f.fields[name]; ok {
		return fi, ok
	}
	if fi, ok := f.fieldsLow[strings.ToLower(name)]; ok {
		return fi, ok
	}
	if fi, ok := f.columns[name]; ok {
		return fi, ok
	}
	return nil, false
}

// create new field info collection
func newFields() *fields {
	f := new(fields)
	f.fields = make(map[string]*fieldInfo)
	f.fieldsLow = make(map[string]*fieldInfo)
	f.columns = make(map[string]*fieldInfo)
	return f
}
