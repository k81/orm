package orm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type aDynContent struct {
	Value int `json:"value"`
}

type bDynContent struct {
	Values []int `json:"values"`
}

type dynamicModel struct {
	ID      int64       `orm:"pk;column(id)"`
	Type    string      `orm:"column(type)"`
	Content interface{} `orm:"column(content);json" dynamic:"true"`
}

func (m *dynamicModel) TableName() string {
	return "dynamic_test"
}

func (m *dynamicModel) NewDynamicField(name string) interface{} {
	switch m.Type {
	case "A":
		return new(aDynContent)
	case "B":
		return new(bDynContent)
	}
	return nil
}

func TestDynamic(t *testing.T) {
	db := NewOrm(context.TODO())
	db.QueryTable(new(dynamicModel)).Delete()

	aObj := &dynamicModel{
		Type: "A",
		Content: &aDynContent{
			Value: 10,
		},
	}
	aId, err := db.Insert(aObj)
	require.NoError(t, err, "insert dyn aObj")
	t.Logf("aObj.ID=%v", aId)

	bObj := &dynamicModel{
		Type: "B",
		Content: &bDynContent{
			Values: []int{1, 3, 5},
		},
	}
	bId, err := db.Insert(bObj)
	require.NoError(t, err, "insert dyn bObj")
	t.Logf("bObj.ID=%v", bId)

	aObjRead := &dynamicModel{ID: aId}
	bObjRead := &dynamicModel{ID: bId}
	err = db.Read(aObjRead)
	require.NoError(t, err, "read dyn aObj")
	require.Equal(t, "A", aObjRead.Type, "check read dyn aObj.Type")
	require.IsType(t, aObj.Content, aObjRead.Content, "check read dyn aObj.Content")
	require.Equal(t, 10, aObjRead.Content.(*aDynContent).Value, "check aObj.Content.Value")

	err = db.Read(bObjRead)
	require.NoError(t, err, "read dyn bObj")
	require.Equal(t, "B", bObjRead.Type, "check read dyn bObj.Type")
	require.IsType(t, bObj.Content, bObjRead.Content, "check read dyn bObj.Content")
	require.Equal(t, []int{1, 3, 5}, bObjRead.Content.(*bDynContent).Values, "check bObj.Content.Value")
}
