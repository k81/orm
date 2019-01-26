package orm

import (
	"bytes"
	"context"
	"fmt"
	"log"
)

// Logger is the orm logger interface
type Logger interface {
	Debug(ctx context.Context, msg string, keyvals ...interface{})
	Info(ctx context.Context, msg string, keyvals ...interface{})
	Warning(ctx context.Context, msg string, keyvals ...interface{})
	Error(ctx context.Context, msg string, keyvals ...interface{})
	Fatal(ctx context.Context, msg string, keyvals ...interface{})
}

var logger Logger = &defaultLogger{}

// SetLogger set the orm logger
func SetLogger(l Logger) {
	logger = l
}

type defaultLogger struct{}

func (l *defaultLogger) Debug(ctx context.Context, msg string, keyvals ...interface{}) {
	log.Println("[DEBUG]", l.format(msg, keyvals))
}
func (l *defaultLogger) Info(ctx context.Context, msg string, keyvals ...interface{}) {
	log.Println("[INFO]", l.format(msg, keyvals))
}
func (l *defaultLogger) Warning(ctx context.Context, msg string, keyvals ...interface{}) {
	log.Println("[WARNING]", l.format(msg, keyvals))
}
func (l *defaultLogger) Error(ctx context.Context, msg string, keyvals ...interface{}) {
	log.Println("[ERROR]", l.format(msg, keyvals))
}
func (l *defaultLogger) Fatal(ctx context.Context, msg string, keyvals ...interface{}) {
	log.Println("[FATAL]", l.format(msg, keyvals))
}
func (l *defaultLogger) format(msg string, keyvals []interface{}) string {
	if len(keyvals) == 0 {
		return ""
	}

	if len(keyvals)%2 != 0 {
		keyvals = append(keyvals, "(MISSING)")
	}

	b := &bytes.Buffer{}
	b.WriteString(msg)
	b.WriteString("||")

	for i := 0; i < len(keyvals); i += 2 {
		key, val := keyvals[i], keyvals[i+1]
		fmt.Fprint(b, key)
		b.WriteString("=")
		fmt.Fprint(b, val)
		b.WriteString("||")
	}

	b.Truncate(b.Len() - 2)

	return b.String()
}
