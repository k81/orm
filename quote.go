package orm

import "fmt"

func quote(field string) string {
	return fmt.Sprintf("`%v`", field)
}

func quoteAll(fields []string) []string {
	quotedFields := make([]string, len(fields))
	for i := range fields {
		quotedFields[i] = quote(fields[i])
	}
	return quotedFields
}
