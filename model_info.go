package orm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// single model info
type modelInfo struct {
	pkg       string
	name      string
	fullName  string
	db        string
	table     string
	model     interface{}
	fields    *fields
	sharded   bool
	addrField reflect.Value //store the original struct value
}

// new model info
func newModelInfo(val reflect.Value) (mi *modelInfo) {
	mi = &modelInfo{}
	mi.fields = newFields()
	ind := reflect.Indirect(val)
	mi.addrField = val
	mi.name = ind.Type().Name()
	mi.fullName = getFullName(ind.Type())
	mi.addFields(ind, "", []int{})
	return
}

// set auto auto field
func (mi *modelInfo) setAutoField(ind reflect.Value, id int64) {
	if mi.fields.auto != nil {
		autoVal := ind.FieldByIndex(mi.fields.auto.fieldIndex)
		switch autoVal.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			autoVal.SetUint(uint64(id))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			autoVal.SetInt(int64(id))
		}
	}
}

// getExistPk return the pk's column and value, and check if has a valid value.
func (mi *modelInfo) getExistPk(ind reflect.Value) (column string, value interface{}, exist bool) {
	fi := mi.fields.pk
	v := ind.FieldByIndex(fi.fieldIndex)
	return fi.column, v.Interface(), v.IsValid()
}

func (mi *modelInfo) getTableByInd(ind reflect.Value) string {
	var tableName string

	if mi.sharded {
		tableSuffix := getTableSuffix(ind.Addr())
		if tableSuffix == "" {
			panic(ErrNoTableSuffix(mi.table))
		}
		tableName = fmt.Sprint(mi.table, "_", tableSuffix)
	} else {
		tableName = mi.table
	}
	return tableName
}

func (mi *modelInfo) getTableBySuffix(suffix string) string {
	if suffix == "" {
		return mi.table
	}

	if !mi.sharded {
		panic(fmt.Errorf("model not sharded: %v", mi.fullName))
	}

	return fmt.Sprint(mi.table, "_", suffix)
}

// index: FieldByIndex returns the nested field corresponding to index
func (mi *modelInfo) addFields(ind reflect.Value, mName string, index []int) {
	var (
		err error
		fi  *fieldInfo
		sf  reflect.StructField
	)

	for i := 0; i < ind.NumField(); i++ {
		field := ind.Field(i)
		sf = ind.Type().Field(i)
		// if the field is unexported skip
		if sf.PkgPath != "" {
			continue
		}
		// add anonymous struct fields
		if sf.Anonymous {
			mi.addFields(field, mName+"."+sf.Name, append(index, i))
			continue
		}

		fi, err = newFieldInfo(mi, field, sf, mName)
		if err == errSkipField {
			err = nil
			continue
		} else if err != nil {
			break
		}
		//record current field index
		fi.fieldIndex = append(fi.fieldIndex, index...)
		fi.fieldIndex = append(fi.fieldIndex, i)
		fi.mi = mi
		if !mi.fields.Add(fi) {
			err = fmt.Errorf("duplicate column name: %s", fi.column)
			break
		}
		if fi.pk {
			if mi.fields.pk != nil {
				err = fmt.Errorf("one model must have one pk field only")
				break
			} else {
				mi.fields.pk = fi
			}
		}
		if fi.auto {
			if mi.fields.auto != nil {
				err = fmt.Errorf("one model must have one auto field only")
				break
			} else {
				mi.fields.auto = fi
			}
		}
	}

	if err != nil {
		panic(fmt.Errorf("field: %s.%s, %s", ind.Type(), sf.Name, err))
	}
}

func (mi *modelInfo) getFieldInfo(anyName string) *fieldInfo {
	fi, ok := mi.fields.GetByAny(anyName)
	if !ok {
		panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", anyName, mi.fullName))
	}
	return fi
}

func (mi *modelInfo) getColumns(anyNames []string) []string {
	names := make([]string, len(anyNames))

	for i, anyName := range anyNames {
		fi, ok := mi.fields.GetByAny(anyName)
		if !ok {
			panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", anyName, mi.fullName))
		}

		names[i] = fi.column
	}
	return names
}

func (mi *modelInfo) getValues(ind reflect.Value, anyNames []string) []interface{} {
	values := make([]interface{}, len(anyNames))
	for i, anyName := range anyNames {
		fi, ok := mi.fields.GetByAny(anyName)
		if !ok {
			panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", anyName, mi.fullName))
		}

		value := ind.FieldByIndex(fi.fieldIndex).Interface()
		if fi.json {
			value = getJSONValue(value, fi.jsonOmitEmpty)
		}

		values[i] = value
	}
	return values
}

// getValueContainers return a containers slice used for row.Scan(containers...)
func (mi *modelInfo) getValueContainers(ind reflect.Value, columns []string) ([]string, []interface{}) {
	dynColumns := []string{}
	containers := make([]interface{}, len(columns))
	for i, column := range columns {
		fi, ok := mi.fields.GetByAny(column)
		if !ok {
			panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", column, mi.fullName))
		}

		field := ind.FieldByIndex(fi.fieldIndex)
		container := field.Addr().Interface()

		if fi.json {
			if fi.dynamic {
				container := new(json.RawMessage)
				dynColumns = append(dynColumns, fi.column)
				field.Set(reflect.ValueOf(container))
			}
			container = getJSONValue(container, fi.jsonOmitEmpty)
		}
		containers[i] = container
	}
	return dynColumns, containers
}

func (mi *modelInfo) setDynamicFields(ind reflect.Value, dynColumns []string) error {
	if len(dynColumns) == 0 {
		return nil
	}

	addrVal := ind
	if ind.CanAddr() && ind.Kind() != reflect.Ptr {
		addrVal = ind.Addr()
	}

	dynFielder, ok := addrVal.Interface().(DynamicFielder)
	if !ok {
		panic(fmt.Errorf("setDynamicFields on non dynamic fielder type: %v", addrVal.Type()))
	}

	for _, dynColumn := range dynColumns {
		fi := mi.fields.GetByColumn(dynColumn)
		if fi == nil {
			panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", dynColumn, mi.fullName))
		}

		field := ind.FieldByIndex(fi.fieldIndex)
		rawMsg, ok := field.Interface().(*json.RawMessage)
		if !ok {
			panic(fmt.Errorf("dynamic field is not scanned as *json.RawMessage: %v", fi.fullName))
		}

		ptr := dynFielder.NewDynamicField(fi.name)

		if err := parseJSON(bytes.NewReader(*rawMsg), ptr); err != nil {
			return err
		}
		field.Set(reflect.ValueOf(ptr))
	}
	return nil
}

// parseExprs parse the field and operator
func (mi *modelInfo) parseExprs(exprs []string) (fi *fieldInfo, operator string, ok bool) {
	if len(exprs) == 0 {
		return
	}

	if fi, ok = mi.fields.GetByAny(exprs[0]); ok {
		if len(exprs) > 1 {
			operator = exprs[1]
		} else {
			operator = "exact"
		}
	}

	return
}

// getOrderByCols builds the order by cols
func (mi *modelInfo) getOrderByCols(orders []string) []string {
	if len(orders) == 0 {
		return nil
	}

	cols := make([]string, 0, len(orders))
	for _, order := range orders {
		direction := "ASC"
		switch order[0] {
		case '-':
			direction = "DESC"
			order = order[1:]
		case '+':
			order = order[1:]
		}

		exprs := strings.Split(order, ExprSep)

		fi, _, ok := mi.parseExprs(exprs)
		if !ok {
			panic(fmt.Errorf("unknown field/column name `%s`", strings.Join(exprs, ExprSep)))
		}

		cols = append(cols, fmt.Sprintf("%s %s", quote(fi.column), direction))
	}

	return cols
}

// getGroupCols builds the group by sql
func (mi *modelInfo) getGroupCols(groups []string) []string {
	if len(groups) == 0 {
		return nil
	}

	cols := make([]string, 0, len(groups))
	for _, group := range groups {
		exprs := strings.Split(group, ExprSep)

		fi, _, ok := mi.parseExprs(exprs)
		if !ok {
			panic(fmt.Errorf("unknown field/column name `%s`", strings.Join(exprs, ExprSep)))
		}

		cols = append(cols, quote(fi.column))
	}

	return cols
}
