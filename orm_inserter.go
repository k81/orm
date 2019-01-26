package orm

import (
	"fmt"
	"reflect"
)

// Inserter insert prepared statement
type Inserter interface {
	Insert(interface{}) (int64, error)
	Close() error
}

var _ Inserter = new(preparedInserter)

// an insert queryer struct
type preparedInserter struct {
	mi     *modelInfo
	orm    *orm
	stmt   StmtQueryer
	closed bool
}

// Insert model ignore it's registered or not.
func (pi *preparedInserter) Insert(md interface{}) (int64, error) {
	if pi.closed {
		return 0, ErrStmtClosed
	}
	val := reflect.ValueOf(md)
	ind := reflect.Indirect(val)
	typ := ind.Type()
	name := getFullName(typ)
	if val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("<Inserter.Insert> cannot use non-ptr model struct `%s`", name))
	}
	if name != pi.mi.fullName {
		panic(fmt.Errorf("<Inserter.Insert> need model `%s` but found `%s`", pi.mi.fullName, name))
	}
	id, err := pi.mi.InsertStmt(pi.orm.ctx, pi.stmt, ind)
	if err != nil {
		return id, err
	}

	pi.mi.setAutoField(ind, id)
	return id, nil
}

// Close insert queryer statement
func (pi *preparedInserter) Close() error {
	if pi.closed {
		return ErrStmtClosed
	}
	pi.closed = true
	return pi.stmt.Close()
}

// newPreparedInserter create new insert queryer.
func newPreparedInserter(orm *orm, mi *modelInfo, tableSuffix string) (Inserter, error) {
	pi := new(preparedInserter)
	pi.orm = orm
	pi.mi = mi
	st, query, err := mi.PrepareInsert(orm.ctx, orm.db, tableSuffix)
	if err != nil {
		return nil, err
	}
	if Debug {
		pi.stmt = newStmtQueryLog(orm.ctx, mi.db, st, query)
	} else {
		pi.stmt = st
	}
	return pi, nil
}
