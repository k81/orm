package orm

import "fmt"

// Params stores the Params
type Params map[string]interface{}

// ParamsList stores paramslist
type ParamsList []interface{}

type colValue struct {
	value int64
	opt   operator
}

type operator int

// define Col operations
const (
	ColAdd operator = iota
	ColMinus
	ColMultiply
	ColExcept
)

// ColValue do the field raw changes. e.g Nums = Nums + 10. usage:
// 	Params{
// 		"Nums": ColValue(Col_Add, 10),
// 	}
func ColValue(opt operator, value interface{}) interface{} {
	switch opt {
	case ColAdd, ColMinus, ColMultiply, ColExcept:
	default:
		panic(fmt.Errorf("orm.ColValue wrong operator"))
	}
	v, err := StrTo(ToStr(value)).Int64()
	if err != nil {
		panic(fmt.Errorf("orm.ColValue doesn't support non string/numeric type, %s", err))
	}
	var val colValue
	val.value = v
	val.opt = opt
	return val
}
