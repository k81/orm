package orm

import (
	"fmt"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

var logger, _ = zap.NewDevelopment()

// SetLogger set the orm logger
func SetLogger(l *zap.Logger) {
	logger = l
}

type mysqlErrLogger struct{}

func (*mysqlErrLogger) Print(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logger.Error("mysql_driver_error", zap.String("error", msg))
}

func init() {
	mysql.SetLogger(&mysqlErrLogger{})
}
