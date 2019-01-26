package orm

import (
	"context"
	"database/sql"
)

// StmtQueryer statement querier
type StmtQueryer interface {
	Close() error
	ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row
}

// db querier
type dbQueryer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// transaction interface
type txer interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// transaction interface
type txEnder interface {
	Commit() error
	Rollback() error
}
