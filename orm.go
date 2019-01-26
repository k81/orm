package orm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// Define common vars
var (
	Debug            = false
	DebugSQLBuilder  = false
	DefaultRowsLimit = 1000
	DefaultTimeLoc   = time.Local
)

// Ormer define the orm interface
type Ormer interface {
	// read data to model
	// for example:
	//	this will find User by Id field
	// 	u = &User{Id: user.Id}
	// 	err = Ormer.Read(u)
	//	this will find User by UserName field
	// 	u = &User{UserName: "astaxie", Password: "pass"}
	//	err = Ormer.Read(u, "UserName")
	Read(md interface{}, cols ...string) error
	// Like Read(), but with "FOR UPDATE" clause, useful in transaction.
	// Some databases are not support this feature.
	ReadForUpdate(md interface{}, cols ...string) error
	// insert model data to database
	// for example:
	//  user := new(User)
	//  id, err = Ormer.Insert(user)
	//  user must a pointer and Insert will set user's pk field
	Insert(interface{}) (int64, error)
	// insert some models to database
	InsertMulti(bulk int, mds interface{}) (int64, error)
	// update model to database.
	// cols set the columns those want to update.
	// find model by Id(pk) field and update columns specified by fields, if cols is null then update all columns
	// for example:
	// user := User{Id: 2}
	//	user.Langs = append(user.Langs, "zh-CN", "en-US")
	//	user.Extra.Name = "beego"
	//	user.Extra.Data = "orm"
	//	num, err = Ormer.Update(&user, "Langs", "Extra")
	Update(md interface{}, cols ...string) (int64, error)
	// delete model in database
	Delete(md interface{}, cols ...string) (int64, error)
	// return a QuerySeter for table operations.
	// table name can be string or struct.
	// e.g. QueryTable(&user{}) or QueryTable((*User)(nil)),
	QueryTable(ptrStruct interface{}) QuerySetter
	// switch to another registered database driver by given name.
	Using(name string)
	// begin transaction
	// for example:
	// 	o := NewOrm()
	// 	err := o.Begin()
	// 	...
	// 	err = o.Rollback()
	Begin() error
	// commit transaction
	Commit() error
	// rollback transaction
	Rollback() error
	// return a raw query seter for raw sql string.
	// for example:
	//	 ormer.Raw("UPDATE `user` SET `user_name` = ? WHERE `user_name` = ?", "slene", "testing").Exec()
	//	// update user testing's name to slene
	Raw(query string, args ...interface{}) RawQueryer
	// RollbackIfNotCommitted as its name explains.
	RollbackIfNotCommitted()
}

var _ Ormer = new(orm)

type orm struct {
	ctx    context.Context
	db     dbQueryer
	dbName string
	isTx   bool
}

// get model info and model reflect value
func (o *orm) getMiInd(md interface{}, needPtr bool) (mi *modelInfo, ind reflect.Value) {
	val := reflect.ValueOf(md)
	ind = reflect.Indirect(val)
	typ := ind.Type()
	if needPtr && val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("<Ormer> cannot use non-ptr model struct `%s`", getFullName(typ)))
	}

	fullName := getFullName(typ)
	mi, ok := modelCache.get(fullName)
	if !ok {
		panic(fmt.Errorf("<Ormer> model `%s` registered", fullName))
	}
	return mi, ind
}

func (o *orm) ReadFromMaster(md interface{}, cols ...string) error {
	mi, ind := o.getMiInd(md, true)
	if !o.isTx {
		o.Using(mi.db)
	}
	return mi.Read(o.ctx, o.db, ind, cols, false, true)
}

// read data to model
func (o *orm) Read(md interface{}, cols ...string) error {
	mi, ind := o.getMiInd(md, true)
	if !o.isTx {
		o.Using(mi.db)
	}
	return mi.Read(o.ctx, o.db, ind, cols, false, false)
}

// read data to model, like Read(), but use "SELECT FOR UPDATE" form
func (o *orm) ReadForUpdate(md interface{}, cols ...string) error {
	mi, ind := o.getMiInd(md, true)
	if !o.isTx {
		o.Using(mi.db)
	}
	return mi.Read(o.ctx, o.db, ind, cols, true, false)
}

// insert model data to database
func (o *orm) Insert(md interface{}) (int64, error) {
	mi, ind := o.getMiInd(md, true)
	if !o.isTx {
		o.Using(mi.db)
	}
	id, err := mi.Insert(o.ctx, o.db, ind)
	if err != nil {
		return id, err
	}

	mi.setAutoField(ind, id)

	return id, nil
}

// insert some models to database
func (o *orm) InsertMulti(bulk int, mds interface{}) (int64, error) {
	sind := reflect.Indirect(reflect.ValueOf(mds))

	switch sind.Kind() {
	case reflect.Array, reflect.Slice:
		if sind.Len() == 0 {
			panic(errors.New("<Ormer> InsertMulti args length is zero"))
		}
	default:
		panic(errors.New("<Ormer> InsertMulti args must be array or slice"))
	}

	tableSuffix := getTableSuffix(sind.Index(0))
	for i := 1; i < sind.Len(); i++ {
		if tableSuffix != getTableSuffix(sind.Index(i)) {
			return 0, ErrTableSuffixNotSameInBatchInsert
		}
	}

	mi, _ := o.getMiInd(sind.Index(0).Interface(), false)
	if !o.isTx {
		o.Using(mi.db)
	}
	return mi.InsertMulti(o.ctx, o.db, sind, bulk, tableSuffix)
}

// update model to database.
// cols set the columns those want to update.
func (o *orm) Update(md interface{}, cols ...string) (int64, error) {
	mi, ind := o.getMiInd(md, true)
	if !o.isTx {
		o.Using(mi.db)
	}
	return mi.Update(o.ctx, o.db, ind, cols)
}

// delete model in database
// cols shows the delete conditions values read from. default is pk
func (o *orm) Delete(md interface{}, cols ...string) (int64, error) {
	mi, ind := o.getMiInd(md, true)
	if !o.isTx {
		o.Using(mi.db)
	}
	return mi.Delete(o.ctx, o.db, ind, cols)
}

func (o *orm) QueryTable(ptrStruct interface{}) QuerySetter {
	typ := reflect.TypeOf(ptrStruct)
	if typ.Kind() != reflect.Ptr {
		panic(fmt.Errorf("<Ormer.QueryTable> must be struct ptr, but got %v", typ))
	}
	elemTyp := typ.Elem()
	if elemTyp.Kind() != reflect.Struct {
		panic(fmt.Errorf("<Ormer.QueryTable> must be struct ptr, but got %v", typ))
	}

	fullName := getFullName(elemTyp)
	mi, ok := modelCache.get(fullName)
	if !ok {
		panic(fmt.Errorf("<Ormer.QueryTable> model not registered: `%s`", fullName))
	}

	qs := newQuerySetter(o, mi)
	return qs
}

func (o *orm) Using(dbName string) {
	if o.isTx {
		panic(fmt.Errorf("<Ormer.Using> transaction has been start, cannot change db"))
	}

	if dbName == o.dbName {
		return
	}

	db, ok := dbCache.get(dbName)
	if !ok {
		db = dbCache.getDefault()
		dbName = "default"
	}

	o.dbName = dbName

	if Debug {
		o.db = newDbQueryLog(o.ctx, dbName, db.DB)
	} else {
		o.db = db.DB
	}
}

// Begin start a new transaction
func (o *orm) Begin() error {
	return o.BeginTx(nil)
}

// begin start a new transaction with tx options
func (o *orm) BeginTx(opt *sql.TxOptions) error {
	if o.isTx {
		return ErrTxHasBegan
	}

	if o.db == nil {
		o.Using("default")
	}

	tx, err := o.db.(txer).BeginTx(o.ctx, opt)
	if err != nil {
		return err
	}
	o.isTx = true
	if Debug {
		o.db.(*dbQueryLog).SetDB(tx)
	} else {
		o.db = tx
	}
	return nil
}

// commit transaction
func (o *orm) Commit() error {
	if !o.isTx {
		return ErrTxDone
	}
	err := o.db.(txEnder).Commit()
	if err == nil {
		o.isTx = false
	} else if err == sql.ErrTxDone {
		return ErrTxDone
	}
	return err
}

// rollback transaction
func (o *orm) Rollback() error {
	if !o.isTx {
		return ErrTxDone
	}
	err := o.db.(txEnder).Rollback()
	if err == nil {
		o.isTx = false
	} else if err == sql.ErrTxDone {
		return ErrTxDone
	}
	return err
}

// return a raw query seter for raw sql string.
func (o *orm) Raw(query string, args ...interface{}) RawQueryer {
	if o.db == nil {
		o.Using("default")
	}
	return newRawQueryer(o, query, args)
}

// RollbackIfNotCommitted as its name explains.
func (o *orm) RollbackIfNotCommitted() {
	if o.isTx {
		// nolint:errcheck
		o.Rollback()
	}
}

// NewOrm create a new orm object.
func NewOrm(ctx context.Context) Ormer {
	BootStrap() // execute only once

	o := new(orm)
	o.ctx = ctx
	return o
}
