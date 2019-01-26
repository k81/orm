package orm

import (
	"context"
	"database/sql"
	"time"
)

func debugLogQueies(
	ctx context.Context,
	dbName string,
	operation,
	query string,
	t time.Time,
	err error,
	args ...interface{},
) {
	var (
		elapsed = int64(time.Since(t) / time.Millisecond)
		flag    = "OK"
	)

	if err != nil {
		flag = "FAIL"
	}

	logger.Debug(ctx, "debug sql",
		"db", dbName,
		"flag", flag,
		"operation", operation,
		"elapsed_ms", elapsed,
		"sql", query,
		"args", args,
		"error", err,
	)
}

// statement query logger struct.
// if dev mode, use stmtQueryLog, or use StmtQueryer.
type stmtQueryLog struct {
	dbName string
	query  string
	stmt   StmtQueryer
	ctx    context.Context
}

var _ StmtQueryer = new(stmtQueryLog)

func (d *stmtQueryLog) Close() error {
	a := time.Now()
	err := d.stmt.Close()
	debugLogQueies(d.ctx, d.dbName, "stmt.Close", d.query, a, err)
	return err
}

func (d *stmtQueryLog) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	a := time.Now()
	res, err := d.stmt.ExecContext(ctx, args...)
	debugLogQueies(ctx, d.dbName, "stmt.ExecContext", d.query, a, err, args...)
	return res, err
}

func (d *stmtQueryLog) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	a := time.Now()
	res, err := d.stmt.QueryContext(ctx, args...)
	debugLogQueies(ctx, d.dbName, "stmt.QueryContext", d.query, a, err, args...)
	return res, err
}

func (d *stmtQueryLog) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	a := time.Now()
	res := d.stmt.QueryRowContext(ctx, args...)
	debugLogQueies(ctx, d.dbName, "stmt.QueryRowContext", d.query, a, nil, args...)
	return res
}

func newStmtQueryLog(ctx context.Context, dbName string, stmt StmtQueryer, query string) StmtQueryer {
	d := new(stmtQueryLog)
	d.ctx = ctx
	d.stmt = stmt
	d.dbName = dbName
	d.query = query
	return d
}

// database query logger struct.
// if dev mode, use dbQueryLog, or use dbQueryer.
type dbQueryLog struct {
	dbName string
	db     dbQueryer
	ctx    context.Context
}

var _ dbQueryer = new(dbQueryLog)
var _ txer = new(dbQueryLog)
var _ txEnder = new(dbQueryLog)

func (d *dbQueryLog) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	a := time.Now()
	stmt, err := d.db.PrepareContext(ctx, query)
	debugLogQueies(ctx, d.dbName, "db.PrepareContext", query, a, err)
	return stmt, err
}

func (d *dbQueryLog) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	a := time.Now()
	res, err := d.db.ExecContext(ctx, query, args...)
	debugLogQueies(ctx, d.dbName, "db.ExecContext", query, a, err, args...)
	return res, err
}

func (d *dbQueryLog) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	a := time.Now()
	res, err := d.db.QueryContext(ctx, query, args...)
	debugLogQueies(ctx, d.dbName, "db.QueryContext", query, a, err, args...)
	return res, err
}

func (d *dbQueryLog) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	a := time.Now()
	res := d.db.QueryRowContext(ctx, query, args...)
	debugLogQueies(ctx, d.dbName, "db.QueryRowContext", query, a, nil, args...)
	return res
}

func (d *dbQueryLog) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	a := time.Now()
	tx, err := d.db.(txer).BeginTx(ctx, opts)
	debugLogQueies(ctx, d.dbName, "db.BeginTx", "START TRANSACTION", a, err)
	return tx, err
}

func (d *dbQueryLog) Commit() error {
	a := time.Now()
	err := d.db.(txEnder).Commit()
	debugLogQueies(d.ctx, d.dbName, "tx.Commit", "COMMIT", a, err)
	return err
}

func (d *dbQueryLog) Rollback() error {
	a := time.Now()
	err := d.db.(txEnder).Rollback()
	debugLogQueies(d.ctx, d.dbName, "tx.Rollback", "ROLLBACK", a, err)
	return err
}

func (d *dbQueryLog) SetDB(db dbQueryer) {
	d.db = db
}

func newDbQueryLog(ctx context.Context, dbName string, db dbQueryer) dbQueryer {
	d := new(dbQueryLog)
	d.ctx = ctx
	d.dbName = dbName
	d.db = db
	return d
}
