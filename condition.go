package orm

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/k81/kate/orm/sqlbuilder"
)

type condValue struct {
	exprs  []string
	args   []interface{}
	cond   *Condition
	isOr   bool
	isNot  bool
	isCond bool
}

// Condition struct.
// work for WHERE conditions.
type Condition struct {
	params []condValue
}

// NewCondition return new condition struct
func NewCondition() *Condition {
	return &Condition{}
}

// And add expression to condition
func (c Condition) And(expr string, args ...interface{}) *Condition {
	if expr == "" || len(args) == 0 {
		panic(fmt.Errorf("<Condition.And> args cannot empty"))
	}
	c.params = append(c.params, condValue{exprs: strings.Split(expr, ExprSep), args: args})
	return &c
}

// AndNot add NOT expression to condition
func (c Condition) AndNot(expr string, args ...interface{}) *Condition {
	if expr == "" || len(args) == 0 {
		panic(fmt.Errorf("<Condition.AndNot> args cannot empty"))
	}
	c.params = append(c.params, condValue{exprs: strings.Split(expr, ExprSep), args: args, isNot: true})
	return &c
}

// AndCond combine a condition to current condition
func (c *Condition) AndCond(cond *Condition) *Condition {
	c = c.clone()
	if c == cond {
		panic(fmt.Errorf("<Condition.AndCond> cannot use self as sub cond"))
	}
	if cond != nil {
		c.params = append(c.params, condValue{cond: cond, isCond: true})
	}
	return c
}

// AndNotCond combine a AND NOT condition to current condition
func (c *Condition) AndNotCond(cond *Condition) *Condition {
	c = c.clone()
	if c == cond {
		panic(fmt.Errorf("<Condition.AndNotCond> cannot use self as sub cond"))
	}

	if cond != nil {
		c.params = append(c.params, condValue{cond: cond, isCond: true, isNot: true})
	}
	return c
}

// Or add OR expression to condition
func (c Condition) Or(expr string, args ...interface{}) *Condition {
	if expr == "" || len(args) == 0 {
		panic(fmt.Errorf("<Condition.Or> args cannot empty"))
	}
	c.params = append(c.params, condValue{exprs: strings.Split(expr, ExprSep), args: args, isOr: true})
	return &c
}

// OrNot add OR NOT expression to condition
func (c Condition) OrNot(expr string, args ...interface{}) *Condition {
	if expr == "" || len(args) == 0 {
		panic(fmt.Errorf("<Condition.OrNot> args cannot empty"))
	}
	c.params = append(c.params, condValue{exprs: strings.Split(expr, ExprSep), args: args, isNot: true, isOr: true})
	return &c
}

// OrCond combine a OR condition to current condition
func (c *Condition) OrCond(cond *Condition) *Condition {
	c = c.clone()
	if c == cond {
		panic(fmt.Errorf("<Condition.OrCond> cannot use self as sub cond"))
	}
	if cond != nil {
		c.params = append(c.params, condValue{cond: cond, isCond: true, isOr: true})
	}
	return c
}

// OrNotCond combine a OR NOT condition to current condition
func (c *Condition) OrNotCond(cond *Condition) *Condition {
	c = c.clone()
	if c == cond {
		panic(fmt.Errorf("<Condition.OrNotCond> cannot use self as sub cond"))
	}

	if cond != nil {
		c.params = append(c.params, condValue{cond: cond, isCond: true, isNot: true, isOr: true})
	}
	return c
}

// IsEmpty check the condition arguments are empty or not.
func (c *Condition) IsEmpty() bool {
	return len(c.params) == 0
}

// GetWhereSQL return the where expr
func (c *Condition) GetWhereSQL(mi *modelInfo, cond *sqlbuilder.Cond) string {
	if c == nil || c.IsEmpty() {
		return ""
	}

	buf := &bytes.Buffer{}
	for i, p := range c.params {
		if i > 0 {
			if p.isOr {
				buf.WriteString(" OR ")
			} else {
				buf.WriteString(" AND ")
			}
		}
		if p.isNot {
			buf.WriteString("NOT ")
		}
		if p.isCond {
			sql := p.cond.GetWhereSQL(mi, cond)
			if sql != "" {
				buf.WriteString("(")
				buf.WriteString(sql)
				buf.WriteString(")")
			}
		} else {
			fi, operator, ok := mi.parseExprs(p.exprs)
			if !ok {
				panic(fmt.Errorf("unknown field/column name `%s`", strings.Join(p.exprs, ExprSep)))
			}

			sql := c.getOperatorSQL(quote(fi.column), operator, p.args, cond)
			buf.WriteString(sql)
		}
	}

	return buf.String()
}

// nolint:gocyclo
func (c *Condition) getOperatorSQL(column, operator string, args []interface{}, cond *sqlbuilder.Cond) string {
	if len(args) == 0 {
		panic(fmt.Errorf("operator `%s` need at least one args", operator))
	}

	var sql string
	switch operator {
	case "in":
		if len(args) == 1 {
			args = c.flatArgs(args[0])
		}
		sql = cond.In(column, args...)
	case "between":
		if len(args) != 2 {
			panic(fmt.Errorf("operator `%v` need 2 args not %d", operator, len(args)))
		}
		sql = cond.Between(column, args[0], args[1])
	case "lt":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		sql = cond.L(column, args[0])
	case "lte":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		sql = cond.LE(column, args[0])
	case "gt":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		sql = cond.G(column, args[0])
	case "gte":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		sql = cond.GE(column, args[0])
	case "exact", "eq":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		sql = cond.E(column, args[0])
	case "ne":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		sql = cond.NE(column, args[0])
	case "iexact":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		param := strings.Replace(ToStr(args[0]), `%`, `\%`, -1)
		sql = cond.Like(column, param)
	case "contains":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		param := strings.Replace(ToStr(args[0]), `%`, `\%`, -1)
		param = fmt.Sprintf("%%%s%%", param)
		sql = cond.LikeBinary(column, param)
	case "icontains":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		param := strings.Replace(ToStr(args[0]), `%`, `\%`, -1)
		param = fmt.Sprintf("%%%s%%", param)
		sql = cond.Like(column, param)
	case "startswith":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		param := strings.Replace(ToStr(args[0]), `%`, `\%`, -1)
		param = fmt.Sprintf("%s%%", param)
		sql = cond.LikeBinary(column, param)
	case "istartswith":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		param := strings.Replace(ToStr(args[0]), `%`, `\%`, -1)
		param = fmt.Sprintf("%s%%", param)
		sql = cond.Like(column, param)
	case "endswith":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		param := strings.Replace(ToStr(args[0]), `%`, `\%`, -1)
		param = fmt.Sprintf("%%%s", param)
		sql = cond.LikeBinary(column, param)
	case "iendswith":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		param := strings.Replace(ToStr(args[0]), `%`, `\%`, -1)
		param = fmt.Sprintf("%%%s", param)
		sql = cond.Like(column, param)
	case "isnull":
		if len(args) > 1 {
			panic(fmt.Errorf("operator `%v` need 1 args not %d", operator, len(args)))
		}
		b, ok := args[0].(bool)
		if !ok {
			panic(fmt.Errorf("operator `%v` need a bool value not `%T`", operator, args[0]))
		}
		if b {
			sql = cond.IsNull(column)
		} else {
			sql = cond.IsNotNull(column)
		}
	default:
		panic(fmt.Errorf("operator `%v` unknown", operator))
	}

	return sql
}

func (c Condition) flatArgs(arg interface{}) []interface{} {
	val := reflect.ValueOf(arg)
	kind := val.Kind()
	if kind == reflect.Ptr {
		val = val.Elem()
		kind = val.Kind()
		arg = val.Interface()
	}

	if kind != reflect.Slice && kind != reflect.Array {
		return []interface{}{arg}
	}

	if _, isBytes := arg.([]byte); isBytes {
		return []interface{}{arg}
	}

	args := make([]interface{}, 0, val.Len())
	for i := 0; i < val.Len(); i++ {
		v := val.Index(i)

		var vu interface{}
		if v.CanInterface() {
			vu = v.Interface()
		}

		if vu == nil {
			continue
		}

		args = append(args, vu)
	}

	return args
}

// clone clone a condition
func (c Condition) clone() *Condition {
	return &c
}
