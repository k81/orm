package orm

import (
	"errors"
	"fmt"
)

var (
	// ErrMissPK indicates missing pk error
	ErrMissPK = errors.New("missed pk value")

	// ErrTxHasBegan indicates tx already begin
	ErrTxHasBegan = errors.New("<Ormer.Begin> transaction already begin")

	// ErrTxDone indicates tx already done
	ErrTxDone = errors.New("<Ormer.Commit/Rollback> transaction not begin")

	// ErrMultiRows indicates multi rows returned
	ErrMultiRows = errors.New("<QuerySeter> return multi rows")

	// ErrNoRows indicates no row found
	ErrNoRows = errors.New("<QuerySeter> no row found")

	// ErrStmtClosed indicates stmt already closed
	ErrStmtClosed = errors.New("<QuerySeter> stmt already closed")

	// ErrTableSuffixNotSameInBatchInsert indicates table suffix not same in batch insertion
	ErrTableSuffixNotSameInBatchInsert = errors.New("<Ormer> table suffix not same in batch insert")

	// ErrNotImplement indicates function not implemented
	ErrNotImplement = errors.New("have not implement")
)

// ErrNoTableSuffix indicates not table suffix is provided for sharded model
func ErrNoTableSuffix(table string) error {
	return fmt.Errorf("table %s is sharded but no suffix provided", table)
}
