package orm

import "github.com/k81/log"

var logger *log.Logger = log.GetLogger()

// SetLogger set the orm logger
func SetLogger(l *log.Logger) {
	logger = l
}
