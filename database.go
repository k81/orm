package orm

import (
	"database/sql"
	"fmt"
	"time"
)

type database struct {
	Name            string
	DataSource      string
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
	DB              *sql.DB
}

// getDbAlias find the database alias by name
func getDB(name string) *database {
	if db, ok := dbCache.get(name); ok {
		return db
	}
	panic(fmt.Errorf("unknown database name %v", name))
}

// RegisterDB Setting the database connect params. Use the database driver self dataSource args.
func RegisterDB(dbName, driverName, dataSource string, params ...interface{}) {
	db := new(database)
	db.Name = dbName
	db.DataSource = dataSource

	var err error
	if db.DB, err = sql.Open(driverName, dataSource); err != nil {
		panic(fmt.Errorf("register db `%v`, %v", dbName, err))
	}

	if dbCache.add(dbName, db) == false {
		panic(fmt.Errorf("database name `%v` already registered, cannot reuse", dbName))
	}

	for i, v := range params {
		switch i {
		case 0:
			SetMaxIdleConns(db.Name, v.(int))
		case 1:
			SetMaxOpenConns(db.Name, v.(int))
		case 2:
			SetConnMaxLifetime(db.Name, v.(time.Duration))
		}
	}
}

// SetMaxIdleConns Change the max idle conns for *sql.DB, use specify database alias name
func SetMaxIdleConns(dbName string, maxIdleConns int) {
	db := getDB(dbName)

	db.MaxIdleConns = maxIdleConns
	db.DB.SetMaxIdleConns(maxIdleConns)
}

// SetMaxOpenConns Change the max open conns for *sql.DB, use specify database alias name
func SetMaxOpenConns(dbName string, maxOpenConns int) {
	db := getDB(dbName)

	db.MaxOpenConns = maxOpenConns
	db.DB.SetMaxOpenConns(maxOpenConns)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
func SetConnMaxLifetime(dbName string, d time.Duration) {
	db := getDB(dbName)

	db.ConnMaxLifetime = d
	db.DB.SetConnMaxLifetime(d)
}
