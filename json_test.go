package orm

import (
	"context"
	"testing"

	"github.com/k81/dynamicjson"
	"github.com/stretchr/testify/require"
)

type aContent struct {
	Value int `json:"value"`
}

type bContent struct {
	Values []int `json:"items"`
}

type jsonValue struct {
	dynamicjson.DynamicJSON
}

func (jc *jsonValue) NewDynamicContent(typ string) interface{} {
	switch typ {
	case "a":
		return &aContent{}
	case "b":
		return &bContent{}
	}
	return nil
}

type jsonModel struct {
	ID         int64      `json:"id" orm:"pk;column(id)"`
	Content    jsonValue  `json:"content" orm:"column(content);json"`
	ContentPtr *jsonValue `json:"content_ptr" orm:"column(content_ptr);json"`
}

func (*jsonModel) TableName() string {
	return "json_test"
}

func TestDynamicJSON(t *testing.T) {
	db := NewOrm(context.TODO())
	db.QueryTable(new(jsonModel)).Delete()

	aJsonContent := jsonValue{}
	aJsonContent.SetType("a")
	aJsonContent.SetContent(&aContent{Value: 10})
	aObj := &jsonModel{
		Content:    aJsonContent,
		ContentPtr: &aJsonContent,
	}

	aId, err := db.Insert(aObj)
	require.NoError(t, err, "insert aObj")
	t.Logf("aObj.ID=%v", aId)

	bJsonContent := jsonValue{}
	bJsonContent.SetType("b")
	bJsonContent.SetContent(&bContent{Values: []int{1, 3, 5}})
	bObj := &jsonModel{
		Content:    bJsonContent,
		ContentPtr: &bJsonContent,
	}

	bId, err := db.Insert(bObj)
	require.NoError(t, err, "insert bObj")
	t.Logf("bObj.ID=%v", bId)

	aObjRead := &jsonModel{ID: aId}
	bObjRead := &jsonModel{ID: bId}

	err = db.Read(aObjRead)
	require.NoError(t, err, "read aObj")
	require.IsType(t, aObj.Content, aObjRead.Content)
	require.Equal(t, aObj.Content.GetType(), aObjRead.Content.GetType())
	adata := aObjRead.Content.GetContent().(*aContent)
	require.Equal(t, 10, adata.Value, "check aObjRead.Content.Value")
	adataFromPtr := aObjRead.ContentPtr.GetContent().(*aContent)
	require.Equal(t, 10, adataFromPtr.Value, "check aObjRead.ContentPtr.Value")

	err = db.Read(bObjRead)
	require.NoError(t, err, "read bObj")
	require.IsType(t, bObj.Content, bObjRead.Content)
	require.Equal(t, bObj.Content.GetType(), bObjRead.Content.GetType())
	bdata := bObjRead.Content.GetContent().(*bContent)
	require.Equal(t, []int{1, 3, 5}, bdata.Values, "check aObjRead.Content.Values")
	bdataFromPtr := bObjRead.ContentPtr.GetContent().(*bContent)
	require.Equal(t, []int{1, 3, 5}, bdataFromPtr.Values, "check aObjRead.ContentPtr.Values")
}

type mapJsonModel struct {
	ID      int64                  `json:"id" orm:"pk;column(id)"`
	Content map[string]interface{} `json:"content" orm:"column(content);json"`
}

func (*mapJsonModel) TableName() string {
	return "json_test2"
}

func TestJSONMap(t *testing.T) {
	db := NewOrm(context.TODO())
	db.QueryTable(new(mapJsonModel)).Delete()

	content := map[string]interface{}{
		"zhangsan": "1",
		"lisi":     "200",
	}
	mapObj := &mapJsonModel{Content: content}
	id, err := db.Insert(mapObj)
	require.NoError(t, err, "insert map json obj")

	mapObjRead := &mapJsonModel{ID: id}
	err = db.Read(mapObjRead)
	require.NoError(t, err, "read map json obj")
	require.Equal(t, content, mapObjRead.Content, "check map json content readed")
}
