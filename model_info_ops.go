package orm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/k81/kate/orm/sqlbuilder"
)

const (
	// ExprSep define the expression separation
	ExprSep = "__"
	// HintRouterMaster define the router hint for `force master`
	HintRouterMaster = `{"router":"m"} `
)

func (mi *modelInfo) PrepareInsert(ctx context.Context, db dbQueryer, tableSuffix string) (StmtQueryer, string, error) {
	if mi.sharded && tableSuffix == "" {
		panic(ErrNoTableSuffix(mi.table))
	}

	table := mi.getTableBySuffix(tableSuffix)
	builder := sqlbuilder.NewInsertBuilder()

	values := make([]interface{}, len(mi.fields.dbcols))
	for i := 0; i < len(values); i++ {
		values[i] = nil
	}

	builder.InsertInto(quote(table)).
		Cols(quoteAll(mi.fields.dbcols)...).
		Values(values...)

	query, _ := builder.Build()

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:prepare_insert", "query", query)
	}

	stmt, err := db.PrepareContext(ctx, query)
	return stmt, query, err
}

func (mi *modelInfo) InsertStmt(ctx context.Context, stmt StmtQueryer, ind reflect.Value) (int64, error) {
	values := mi.getValues(ind, mi.fields.dbcols)
	result, err := stmt.ExecContext(ctx, values...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (mi *modelInfo) Read(ctx context.Context, db dbQueryer, ind reflect.Value, whereNames []string,
	forUpdate bool, forceMaster bool) error {
	var (
		whereColumns []string
		whereValues  []interface{}
		table        = mi.getTableByInd(ind)
	)

	if len(whereNames) > 0 {
		whereColumns = mi.getColumns(whereNames)
		whereValues = mi.getValues(ind, whereNames)
	} else {
		// default use pk value as whereNames condtion.
		pkColumn, pkValue, ok := mi.getExistPk(ind)
		if !ok {
			return ErrMissPK
		}
		whereColumns = []string{pkColumn}
		whereValues = []interface{}{pkValue}
	}

	builder := sqlbuilder.NewSelectBuilder()

	builder.Select(quoteAll(mi.fields.dbcols)...).
		From(quote(table)).
		Where(getEqualWhereExprs(&builder.Cond, quoteAll(whereColumns), whereValues)...)

	if forUpdate {
		builder.ForUpdate()
	}

	var (
		query string
		args  []interface{}
	)

	if forceMaster {
		query, args = sqlbuilder.Build(HintRouterMaster, builder).Build()
	} else {
		query, args = builder.Build()
	}

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:read", "query", query, "args", args)
	}

	dynColumns, containers := mi.getValueContainers(ind, mi.fields.dbcols)
	err := db.QueryRowContext(ctx, query, args...).Scan(containers...)
	switch {
	case err == sql.ErrNoRows:
		return ErrNoRows
	case err != nil:
		return err
	}

	if err = mi.setDynamicFields(ind, dynColumns); err != nil {
		return err
	}
	return nil
}

func (mi *modelInfo) Insert(ctx context.Context, db dbQueryer, ind reflect.Value) (int64, error) {
	table := mi.getTableByInd(ind)
	values := mi.getValues(ind, mi.fields.dbcols)
	builder := sqlbuilder.NewInsertBuilder()

	builder.InsertInto(quote(table)).Cols(quoteAll(mi.fields.dbcols)...).Values(values...)

	query, args := builder.Build()

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:insert", "query", query, "args", args)
	}

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

func (mi *modelInfo) Update(ctx context.Context, db dbQueryer, ind reflect.Value, setNames []string) (int64, error) {
	pkName, pkValue, ok := mi.getExistPk(ind)
	if !ok {
		return 0, ErrMissPK
	}

	var setColumns []string

	// if specify setNames length is zero, then commit all columns.
	if len(setNames) == 0 {
		setColumns = make([]string, 0, len(mi.fields.dbcols)-1)
		for _, fi := range mi.fields.fieldsDB {
			if !fi.pk {
				setColumns = append(setColumns, fi.column)
			}
		}
	} else {
		setColumns = mi.getColumns(setNames)
	}

	if len(setColumns) == 0 {
		panic(errors.New("no columns to update"))
	}

	setValues := mi.getValues(ind, setColumns)

	table := mi.getTableByInd(ind)
	builder := sqlbuilder.NewUpdateBuilder()

	builder.Update(quote(table)).
		Set(getAssignments(builder, quoteAll(setColumns), setValues)...).
		Where(builder.E(quote(pkName), pkValue))

	query, args := builder.Build()

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:update", "query", query, "args", args)
	}

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (mi *modelInfo) Delete(ctx context.Context, db dbQueryer, ind reflect.Value, whereNames []string) (int64, error) {
	var (
		whereColumns []string
		whereValues  []interface{}
		table        = mi.getTableByInd(ind)
	)

	// if specify whereNames length > 0, then use it for where condition.
	if len(whereNames) > 0 {
		whereColumns = mi.getColumns(whereNames)
		whereValues = mi.getValues(ind, whereNames)
	} else {
		// default use pk value as where condtion.
		pkColumn, pkValue, ok := mi.getExistPk(ind)
		if !ok {
			return 0, ErrMissPK
		}
		whereColumns = []string{pkColumn}
		whereValues = []interface{}{pkValue}
	}

	if len(whereColumns) == 0 {
		panic(errors.New("delete no where conditions"))
	}

	builder := sqlbuilder.NewDeleteBuilder()
	builder.DeleteFrom(quote(table)).Where(getEqualWhereExprs(&builder.Cond, quoteAll(whereColumns), whereValues)...)

	query, args := builder.Build()

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:delete", "query", query, "args", args)
	}

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func (mi *modelInfo) InsertMulti(
	ctx context.Context,
	db dbQueryer,
	sind reflect.Value,
	bulk int,
	tableSuffix string,
) (int64, error) {
	var (
		table   = mi.getTableBySuffix(tableSuffix)
		builder *sqlbuilder.InsertBuilder
		length  = sind.Len()
		count   int64
	)

	if length == 0 {
		return 0, nil
	}

	bulkIdx := 0
	for i := 1; i <= length; i++ {
		if builder == nil {
			builder = sqlbuilder.NewInsertBuilder().InsertInto(quote(table)).Cols(quoteAll(mi.fields.dbcols)...)
		}

		ind := reflect.Indirect(sind.Index(i - 1))
		values := mi.getValues(ind, mi.fields.dbcols)
		builder.Values(values...)

		if i%bulk == 0 || i == length {
			bulkIdx++

			query, args := builder.Build()

			if DebugSQLBuilder {
				logger.Debug(ctx, "sqlbuilder:insert_multi", "query", query, "args", args, "bulk_idx", bulkIdx)
			}

			_, err := db.ExecContext(ctx, query, args...)
			if err != nil {
				return count, err
			}
			count += int64(i % bulk)
			builder = nil
		}
	}

	return count, nil
}

func (mi *modelInfo) UpdateBatch(ctx context.Context, db dbQueryer,
	qs *querySetter, cond *Condition, params Params) (int64, error) {
	setNames := make([]string, 0, len(params))
	setValues := make([]interface{}, 0, len(params))
	for name, value := range params {
		setNames = append(setNames, name)
		setValues = append(setValues, value)
	}
	setColumns := mi.getColumns(setNames)

	table := mi.getTableBySuffix(qs.tableSuffix)
	builder := sqlbuilder.NewUpdateBuilder()

	builder.Update(quote(table)).
		Set(getAssignments(builder, quoteAll(setColumns), setValues)...)

	if cond != nil && !cond.IsEmpty() {
		builder.Where(cond.GetWhereSQL(mi, &builder.Cond))
	}

	query, args := builder.Build()

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:update_batch", "query", query, "args", args)
	}

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (mi *modelInfo) DeleteBatch(ctx context.Context, db dbQueryer, qs *querySetter, cond *Condition) (int64, error) {
	table := mi.getTableBySuffix(qs.tableSuffix)
	builder := sqlbuilder.NewDeleteBuilder()

	builder.DeleteFrom(quote(table))

	if cond != nil && !cond.IsEmpty() {
		builder.Where(cond.GetWhereSQL(mi, &builder.Cond))
	}

	query, args := builder.Build()

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:delete_batch", "query", query, "args", args)
	}

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// nolint:gocyclo,lll
func (mi *modelInfo) getQueryArgsForRead(qs *querySetter, cond *Condition, selectNames []string) (string, []interface{}) {
	var selectColumns []string
	if len(selectNames) > 0 {
		selectColumns = mi.getColumns(selectNames)
	} else {
		selectColumns = mi.fields.dbcols
	}

	builder := sqlbuilder.NewSelectBuilder()
	table := mi.getTableBySuffix(qs.tableSuffix)

	if qs.distinct {
		builder.Distinct()
	}

	builder.Select(quoteAll(selectColumns)...).From(quote(table))

	if cond != nil && !cond.IsEmpty() {
		builder.Where(cond.GetWhereSQL(mi, &builder.Cond))
	}

	if len(qs.orders) > 0 {
		builder.OrderBy(mi.getOrderByCols(qs.orders)...)
	}

	if len(qs.groups) > 0 {
		builder.GroupBy(mi.getGroupCols(qs.groups)...)
	}

	if qs.limit > 0 {
		builder.Limit(qs.limit)
	}

	if qs.offset > 0 {
		builder.Offset(qs.offset)
	}

	if qs.forUpdate {
		builder.ForUpdate()
	}

	var (
		query string
		args  []interface{}
	)
	if qs.forceMaster {
		query, args = sqlbuilder.Build(HintRouterMaster, builder).Build()
	} else {
		query, args = builder.Build()
	}
	return query, args
}

// nolint:lll
func (mi *modelInfo) ReadOne(ctx context.Context, db dbQueryer, qs *querySetter, cond *Condition, container interface{}, selectNames []string) error {
	val := reflect.ValueOf(container)
	ind := reflect.Indirect(val)

	if val.Kind() != reflect.Ptr || mi.fullName != getFullName(ind.Type()) {
		panic(fmt.Errorf("wrong object type `%s` for rows scan, need *%s", val.Type(), mi.fullName))
	}

	if len(selectNames) == 0 {
		selectNames = mi.fields.dbcols
	}

	query, args := mi.getQueryArgsForRead(qs, cond, selectNames)

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:read_one", "query", query, "args", args)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	// nolint:errcheck
	defer rows.Close()

	if rows.Next() {
		elem := reflect.New(mi.addrField.Elem().Type())
		elemInd := reflect.Indirect(elem)

		dynColumns, containers := mi.getValueContainers(elemInd, selectNames)
		if err = rows.Scan(containers...); err != nil {
			return err
		}

		if err = mi.setDynamicFields(elemInd, dynColumns); err != nil {
			return err
		}

		ind.Set(elemInd)
	}

	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}

// nolint:gocyclo,lll
func (mi *modelInfo) ReadBatch(ctx context.Context, db dbQueryer, qs *querySetter, cond *Condition, container interface{}, selectNames []string) error {
	val := reflect.ValueOf(container)
	ind := reflect.Indirect(val)
	isPtr := true

	if val.Kind() != reflect.Ptr || ind.Kind() != reflect.Slice || ind.Len() > 0 {
		panic(fmt.Errorf("wrong object type `%s` for rows scan, need and empty slice *[]*%s or *[]%s",
			val.Type(),
			mi.fullName,
			mi.fullName))
	}

	fn := ""
	typ := ind.Type().Elem()
	switch typ.Kind() {
	case reflect.Ptr:
		fn = getFullName(typ.Elem())
	case reflect.Struct:
		isPtr = false
		fn = getFullName(typ)
	}

	if mi.fullName != fn {
		panic(fmt.Errorf("wrong object type `%s` for rows scan, need *[]*%s or *[]%s",
			val.Type(),
			mi.fullName,
			mi.fullName))
	}

	if len(selectNames) == 0 {
		selectNames = mi.fields.dbcols
	}

	query, args := mi.getQueryArgsForRead(qs, cond, selectNames)

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:read_batch", "query", query, "args", args)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	// nolint:errcheck
	defer rows.Close()

	slice := reflect.New(ind.Type()).Elem()
	for rows.Next() {
		elem := reflect.New(mi.addrField.Elem().Type())
		elemInd := reflect.Indirect(elem)

		dynColumns, containers := mi.getValueContainers(elemInd, selectNames)
		if err = rows.Scan(containers...); err != nil {
			return err
		}

		if err = mi.setDynamicFields(elemInd, dynColumns); err != nil {
			return err
		}

		if isPtr {
			slice = reflect.Append(slice, elemInd.Addr())
		} else {
			slice = reflect.Append(slice, elemInd)
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	ind.Set(slice)

	return nil
}

// nolint:lll
func (mi *modelInfo) Count(ctx context.Context, db dbQueryer, qs *querySetter, cond *Condition) (count int64, err error) {
	table := mi.getTableBySuffix(qs.tableSuffix)
	builder := sqlbuilder.NewSelectBuilder()
	builder.Select("COUNT(1)").From(quote(table))

	if cond != nil && !cond.IsEmpty() {
		builder.Where(cond.GetWhereSQL(mi, &builder.Cond))
	}

	query, args := builder.Build()

	if DebugSQLBuilder {
		logger.Debug(ctx, "sqlbuilder:count", "query", query, "args", args)
	}

	err = db.QueryRowContext(ctx, query, args...).Scan(&count)
	return
}

// getEqualWhereExprs return where exprs used in sqlbuilder.Cond
func getEqualWhereExprs(cond *sqlbuilder.Cond, columns []string, values []interface{}) []string {
	whereExprs := make([]string, len(columns))
	for i := range columns {
		whereExprs[i] = cond.E(columns[i], values[i])
	}
	return whereExprs
}

// getAssignments return set exprs used in sqlbuilder.UpdateBuilder
func getAssignments(ub *sqlbuilder.UpdateBuilder, columns []string, values []interface{}) []string {
	assignments := make([]string, len(columns))
	for i := range columns {
		assignments[i] = ub.Assign(columns[i], values[i])
	}
	return assignments
}
