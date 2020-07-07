package orm

import (
	"fmt"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

var (
	defaultLogger    = zap.NewNop()
	defaultLoggerTag = zap.String("tag", "debug_sql")
)

// SetDefaultLogger set the orm logger
func SetDefaultLogger(l *zap.Logger) {
	defaultLogger = l
}

// SetDefaultLoggerTag set the logger tag
func SetDefaultLoggerTag(tag zap.Field) {
	defaultLoggerTag = tag
}

type mysqlErrLogger struct{}

func (*mysqlErrLogger) Print(v ...interface{}) {
	msg := fmt.Sprint(v...)
	defaultLogger.Error("mysql_driver_error", zap.String("error", msg))
}

func init() {
	mysql.SetLogger(&mysqlErrLogger{})
}
