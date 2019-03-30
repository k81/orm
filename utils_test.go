package orm

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type Obj struct {
	Value interface{} `json:"value,omitempty"`
}

func TestIsEmptyValue(t *testing.T) {
	var pv *int
	var ppv **int = &pv
	var spv []*int = []*int{pv}
	var obj = &Obj{
		Value: spv,
	}
	require.True(t, IsEmptyValue(reflect.ValueOf(pv)))
	require.True(t, IsEmptyValue(reflect.ValueOf(ppv)))
	require.True(t, IsEmptyValue(reflect.ValueOf(spv)))
	require.True(t, IsEmptyValue(reflect.ValueOf(obj)))
	require.True(t, IsEmptyValue(reflect.ValueOf("")))
	require.True(t, IsEmptyValue(reflect.ValueOf(0)))
}
