package orm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// RawStmtQueryer statement querier
type RawStmtQueryer interface {
	Close() error
	Exec(args ...interface{}) (sql.Result, error)
	Query(args ...interface{}) (*sql.Rows, error)
	QueryRow(args ...interface{}) *sql.Row
}

// RawQueryer raw query seter
// create From Ormer.Raw
// for example:
//  sql := fmt.Sprintf("SELECT %sid%s,%sname%s FROM %suser%s WHERE id = ?",Q,Q,Q,Q,Q,Q)
//  rs := Ormer.Raw(sql, 1)
type RawQueryer interface {
	// Exec execute sql and get result
	Exec() (sql.Result, error)

	// QueryRow query data and map to container
	QueryRow(containers ...interface{}) error

	// QueryRows query data rows and map to container
	QueryRows(container interface{}) error

	// SetArgs set args
	SetArgs(...interface{}) RawQueryer

	// Prepare return prepared raw statement for used in times.
	// for example:
	// 	pre, err := dORM.Raw("INSERT INTO tag (name) VALUES (?)").Prepare()
	// 	r, err := pre.Exec("name1") // INSERT INTO tag (name) VALUES (`name1`)
	Prepare() (RawStmtQueryer, error)
}

// rawstmt raw sql string prepared statement
type rawStmt struct {
	rq     *rawQueryer
	stmt   StmtQueryer
	closed bool
}

func (rs *rawStmt) Close() error {
	rs.closed = true
	return rs.stmt.Close()
}

func (rs *rawStmt) Exec(args ...interface{}) (sql.Result, error) {
	if rs.closed {
		panic(ErrStmtClosed)
	}
	return rs.stmt.ExecContext(rs.rq.ctx, args...)
}

func (rs *rawStmt) Query(args ...interface{}) (*sql.Rows, error) {
	if rs.closed {
		panic(ErrStmtClosed)
	}
	return rs.stmt.QueryContext(rs.rq.ctx, args...)
}

func (rs *rawStmt) QueryRow(args ...interface{}) *sql.Row {
	if rs.closed {
		panic(ErrStmtClosed)
	}
	return rs.stmt.QueryRowContext(rs.rq.ctx, args...)
}

func newRawStmt(rq *rawQueryer) (RawStmtQueryer, error) {
	rs := new(rawStmt)
	rs.rq = rq

	query := rq.query

	stmt, err := rq.orm.db.PrepareContext(rq.ctx, query)
	if err != nil {
		return nil, err
	}
	if Debug {
		rs.stmt = newStmtQueryLog(rq.ctx, rq.orm.dbName, stmt, query)
	} else {
		rs.stmt = stmt
	}
	return rs, nil
}

// rawQueryer is the raw queryer
type rawQueryer struct {
	query string
	args  []interface{}
	orm   *orm
	ctx   context.Context
}

var _ RawQueryer = new(rawQueryer)

// SetArgs set args for every query
func (rq rawQueryer) SetArgs(args ...interface{}) RawQueryer {
	rq.args = args
	return &rq
}

// Exec execute raw sql and return sql.Result
func (rq *rawQueryer) Exec() (sql.Result, error) {
	return rq.orm.db.ExecContext(rq.ctx, rq.query, rq.args...)
}

// QueryRow query data and map to container
func (rq *rawQueryer) QueryRow(containers ...interface{}) error {
	err := rq.orm.db.QueryRowContext(rq.ctx, rq.query, rq.args...).Scan(containers...)
	switch {
	case err == sql.ErrNoRows:
		return ErrNoRows
	case err != nil:
		return err
	}

	return nil
}

// QueryRows query data rows and map to container
// nolint:gocyclo
func (rq *rawQueryer) QueryRows(container interface{}) error {
	var (
		val = reflect.ValueOf(container)
		ind = reflect.Indirect(val)

		isPtr    = true
		fullName string
	)

	if val.Kind() != reflect.Ptr || ind.Kind() != reflect.Slice || ind.Len() != 0 {
		panic(fmt.Errorf("<RawQueryer> QueryRows() container should be a ptr of empty struct slice"))
	}

	typ := ind.Type().Elem()
	switch typ.Kind() {
	case reflect.Ptr:
		fullName = getFullName(typ.Elem())
	case reflect.Struct:
		isPtr = false
		fullName = getFullName(typ)
	default:
		panic(fmt.Errorf("<RawQueryer> QueryRows() container should be a ptr of empty struct slice"))
	}

	mi, ok := modelCache.get(fullName)
	if !ok {
		panic(fmt.Errorf("<RawQueryer> model `%v` not registered", fullName))
	}

	rows, err := rq.orm.db.QueryContext(rq.ctx, rq.query, rq.args...)
	if err != nil {
		return err
	}
	// nolint:errcheck
	defer rows.Close()

	var (
		columns []string
		slice   = reflect.New(ind.Type()).Elem()
	)

	for rows.Next() {
		if len(columns) == 0 {
			columns, err = rows.Columns()
			if err != nil {
				return err
			}
		}

		elem := reflect.New(mi.addrField.Elem().Type())
		elemInd := reflect.Indirect(elem)
		dynColumns, containers := mi.getValueContainers(elemInd, columns)

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

// return prepared raw statement for used in times.
func (rq *rawQueryer) Prepare() (RawStmtQueryer, error) {
	return newRawStmt(rq)
}

func newRawQueryer(orm *orm, query string, args []interface{}) RawQueryer {
	q := new(rawQueryer)
	q.query = query
	q.args = args
	q.orm = orm
	q.ctx = orm.ctx
	return q
}
