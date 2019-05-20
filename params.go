package orm

import "fmt"

// Params stores the Params
type Params map[string]interface{}

// ParamsList stores paramslist
type ParamsList []interface{}

type colValue struct {
	value int64
	op    operator
}

type operator int

// define Col operations
const (
	ColAdd operator = iota
	ColSub
	ColMul
	ColDiv
)

// ColValue do the field raw changes. e.g Nums = Nums + 10. usage:
// 	Params{
// 		"Nums": ColValue(Col_Add, 10),
// 	}
func ColValue(op operator, value interface{}) interface{} {
	switch op {
	case ColAdd, ColSub, ColMul, ColDiv:
	default:
		panic(fmt.Errorf("orm.ColValue wrong operator"))
	}
	v, err := StrTo(ToStr(value)).Int64()
	if err != nil {
		panic(fmt.Errorf("orm.ColValue doesn't support non string/numeric type, %s", err))
	}
	return &colValue{
		value: v,
		op:    op,
	}
}
