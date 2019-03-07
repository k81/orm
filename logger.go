package orm

import "github.com/k81/log"

var logger *log.Logger = log.DefaultLogger

// SetLogger set the orm logger
func SetLogger(l *log.Logger) {
	logger = l
}
