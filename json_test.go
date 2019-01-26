package orm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type aData struct {
	Value int `json:"value"`
}

type bData struct {
	Values []int `json:"items"`
}

type jsonContent struct {
	Type string      `json:"type"`
	Data interface{} `json:"data" dynamic:"true"`
}

func (c *jsonContent) NewDynamicField(name string) interface{} {
	switch c.Type {
	case "a":
		return new(aData)
	case "b":
		return new(bData)
	}
	return nil
}

type jsonModel struct {
	ID      int64       `json:"id" orm:"pk;column(id)"`
	Content jsonContent `json:"content" orm:"column(content);json"`
}

func (*jsonModel) TableName() string {
	return "json_test"
}

func TestJSON(t *testing.T) {
	//aRawContent := `{"type": "a", "content": {"value": 10}}`
	//bRawContent := `{"type": "b", "content": {"items": [1,3,5]}}`

	db := NewOrm(context.TODO())
	db.QueryTable(new(jsonModel)).Delete()

	aObj := &jsonModel{
		Content: jsonContent{
			Type: "a",
			Data: &aData{Value: 10},
		},
	}

	aId, err := db.Insert(aObj)
	require.NoError(t, err, "insert aObj")
	t.Logf("aObj.ID=%v", aId)

	bObj := &jsonModel{
		Content: jsonContent{
			Type: "b",
			Data: &bData{Values: []int{1, 3, 5}},
		},
	}

	bId, err := db.Insert(bObj)
	require.NoError(t, err, "insert bObj")
	t.Logf("bObj.ID=%v", bId)

	aObjRead := &jsonModel{ID: aId}
	bObjRead := &jsonModel{ID: bId}

	err = db.Read(aObjRead)
	require.NoError(t, err, "read aObj")
	require.IsType(t, aObj.Content, aObjRead.Content)
	require.Equal(t, aObj.Content.Type, aObjRead.Content.Type)
	adata := aObjRead.Content.Data.(*aData)
	require.Equal(t, 10, adata.Value, "check aObjRead.Content.Value")

	err = db.Read(bObjRead)
	require.NoError(t, err, "read bObj")
	require.IsType(t, bObj.Content, bObjRead.Content)
	require.Equal(t, bObj.Content.Type, bObjRead.Content.Type)
	bdata := bObjRead.Content.Data.(*bData)
	require.Equal(t, []int{1, 3, 5}, bdata.Values, "check aObjRead.Content.Values")
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
