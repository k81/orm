package orm

import (
	"context"
	"os"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

type obj struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

type anyObj struct {
	ID      int64 `orm:"column(id);pk"`
	ObjOmit obj   `orm:"column(obj_omit);json(omitempty)"`
	Obj     obj   `orm:"column(obj);json"`
}

func (o *anyObj) TableName() string {
	return "any_obj"
}

type shardedPerson struct {
	ID        int64  `orm:"column(id);pk;auto"`
	PersonID  int64  `orm:"column(person_id)"`
	Name      string `orm:"column(name)"`
	Age       int    `orm:"column(age)"`
	BirthTime string `orm:"-"`
}

func (*shardedPerson) TableName() string {
	return "person"
}

func (p *shardedPerson) TableSuffix() string {
	return strconv.FormatInt(p.PersonID%4, 10)
}

func TestInsert(t *testing.T) {
	for i := 0; i < 4; i++ {
		tableSuffix := strconv.Itoa(i)
		delCnt, err := NewOrm(context.TODO()).QueryTable(new(shardedPerson)).WithSuffix(tableSuffix).Delete()
		require.NoError(t, err, "delete all rows failed")
		t.Logf("before TestInsert, rows deleted: count=%v", delCnt)
	}

	db := NewOrm(context.TODO())
	person1 := &shardedPerson{
		PersonID: 1,
		Name:     "zhang",
		Age:      18,
	}

	id, err := db.Insert(person1)
	require.NoError(t, err, "insert person1 failed")
	require.Equal(t, id, person1.ID, "check person1.ID")

	person2 := &shardedPerson{
		PersonID: 2,
		Name:     "lisi",
		Age:      20,
	}
	id, err = db.Insert(person2)
	require.NoError(t, err, "insert person2 failed")
	require.Equal(t, id, person2.ID, "check person2.ID")

	person3 := &shardedPerson{
		PersonID: 3,
		Name:     "wang",
		Age:      20,
	}
	id, err = db.Insert(person3)
	require.NoError(t, err, "insert person3 failed")
	require.Equal(t, id, person3.ID, "check person3.ID")

	persons := []*shardedPerson{
		{PersonID: 3, Name: "multi_3"},
		{PersonID: 7, Name: "multi_7"},
		{PersonID: 11, Name: "multi_11"},
	}

	_, err = db.InsertMulti(10, persons)
	require.NoError(t, err, "insertMulti persons failed")

	for i := 0; i < 4; i++ {
		tableSuffix := strconv.Itoa(i)
		delCnt, err := NewOrm(context.TODO()).QueryTable(new(shardedPerson)).WithSuffix(tableSuffix).Delete()
		require.NoError(t, err, "delete all rows failed")
		t.Logf("after TestInsert, rows deleted: count=%v", delCnt)
	}
}

func TestRead(t *testing.T) {
	personAdd := &shardedPerson{
		PersonID: 21,
		Name:     "zhangsan",
		Age:      30,
	}

	// insert person
	db := NewOrm(context.TODO())
	id, err := db.Insert(personAdd)
	require.NoError(t, err, "insert person failed")

	// read by pk
	personReadByPk := &shardedPerson{
		ID:       id,
		PersonID: 1, // fake person id for sharding
	}
	err = db.Read(personReadByPk)
	require.NoError(t, err, "read person by pk failed")
	require.Equal(t, personAdd.PersonID, personReadByPk.PersonID, "person read by pk: check PersonID ")
	require.Equal(t, personAdd.Name, personReadByPk.Name, "person read by pk: check Name ")

	// read by cols
	personReadByCols := &shardedPerson{
		PersonID: 21,
		Name:     "zhangsan",
	}
	err = db.Read(personReadByCols, "PersonID", "Name")
	require.NoError(t, err, "read person by cols failed")
	require.Equal(t, personAdd.ID, personReadByCols.ID, "person read by cols: check pk ID")

	// delete person
	num, err := db.Delete(personAdd)
	require.NoError(t, err, "delete person added failed")
	require.Equal(t, int64(1), num, "delete person added: check num deleted =1")
}

func TestUpdate(t *testing.T) {
	personAdd := &shardedPerson{
		PersonID: 21,
		Name:     "zhangsan",
		Age:      30,
	}

	// insert person
	db := NewOrm(context.TODO())
	id, err := db.Insert(personAdd)
	require.NoError(t, err, "insert person failed")
	require.Equal(t, id, personAdd.ID)

	// update person
	personUpdated := &shardedPerson{
		ID:       personAdd.ID,
		PersonID: 1, // fake sharding id
		Name:     "lisi",
	}
	rowsAffected, err := db.Update(personUpdated, "Name")
	require.NoError(t, err, "update person failed")
	require.Equal(t, int64(1), rowsAffected, "rows affected != 1")

	// reload person
	personReloaded := &shardedPerson{
		ID:       personAdd.ID,
		PersonID: 1, // fake sharding id
	}
	err = db.Read(personReloaded)
	require.NoError(t, err, "reload person failed")
	require.Equal(t, personUpdated.Name, personReloaded.Name)
	require.Equal(t, personAdd.PersonID, personReloaded.PersonID)
	require.Equal(t, personAdd.Age, personReloaded.Age)

	// delete it
	num, err := db.Delete(personAdd)
	require.NoError(t, err, "delete person added failed")
	require.Equal(t, int64(1), num, "delete person added: check num deleted =1")
}

func TestQueryTable(t *testing.T) {
	person1 := &shardedPerson{
		PersonID: 20,
		Name:     "zhangsan",
		Age:      10,
	}
	person2 := &shardedPerson{
		PersonID: 40,
		Name:     "lisi",
		Age:      9,
	}
	db := NewOrm(context.TODO())
	db.Insert(person1)
	db.Insert(person2)

	// Count
	qs := db.QueryTable(new(shardedPerson)).WithSuffix("0").Filter("Age__gte", 9)
	count, err := qs.Count()
	require.NoError(t, err, "query count failed")
	require.Equal(t, int64(2), count, "count != 2")

	// ReadBatch
	var persons []*shardedPerson
	err = qs.OrderBy("-Age").All(&persons)
	require.NoError(t, err, "query all failed")

	require.Equal(t, 2, len(persons), "len(persons) != 2")
	require.Equal(t, person1.PersonID, persons[0].PersonID, "check person[0].PersonID")
	require.Equal(t, person1.Name, persons[0].Name, "check person[0].Name")
	require.Equal(t, person1.Age, persons[0].Age, "check person[0].Age")
	require.Equal(t, person2.PersonID, persons[1].PersonID, "check person[1].PersonID")
	require.Equal(t, person2.Name, persons[1].Name, "check person[1].Name")
	require.Equal(t, person2.Age, persons[1].Age, "check person[1].Age")

	// ReadOne
	personOne := shardedPerson{}
	err = qs.OrderBy("-Age").One(&personOne)
	require.NoError(t, err, "query one failed")
	require.Equal(t, person1.PersonID, personOne.PersonID, "check personOne.PersonID")
	require.Equal(t, person1.Name, personOne.Name, "check personOne.Name")
	require.Equal(t, person1.Age, personOne.Age, "check personOne.Age")

	personOneAsc := shardedPerson{}
	err = qs.OrderBy("Age").One(&personOneAsc)
	require.NoError(t, err, "query one asc failed")
	require.Equal(t, person2.PersonID, personOneAsc.PersonID, "check personOneAsc.PersonID")
	require.Equal(t, person2.Name, personOneAsc.Name, "check personOneAsc.Name")
	require.Equal(t, person2.Age, personOneAsc.Age, "check personOneAsc.Age")

	// UpateBatch
	rowsAffected, err := qs.Update(Params{"Age": 8})
	require.NoError(t, err, "update set age = 8 failed")
	require.Equal(t, int64(2), rowsAffected, "rowsAffected != 2")

	// DeleteBatch
	var personsUpdated []shardedPerson
	err = db.QueryTable(new(shardedPerson)).WithSuffix("0").All(&personsUpdated)
	require.NoError(t, err, "query all after update")
	require.Equal(t, 8, personsUpdated[0].Age, "check personsUpdated[0].Age")
	require.Equal(t, 8, personsUpdated[1].Age, "check personsUpdated[1].Age")

	rowsDeleted, err := db.QueryTable(new(shardedPerson)).WithSuffix("0").Filter("PersonID__in", 20, 40).Delete()
	require.NoError(t, err, "detele for person_id in (20,40)")
	require.Equal(t, int64(2), rowsDeleted, "rowsDeleted != 2")

	rowsDeleted, err = db.QueryTable(new(shardedPerson)).WithSuffix("0").Filter("PersonID__in", []int64{20, 40}).Delete()
	require.NoError(t, err, "2nd detele for person_id in (20,40)")
	require.Equal(t, int64(0), rowsDeleted, "rowsDeleted != 0")
}

func TestJsonOmit(t *testing.T) {
	db := NewOrm(context.TODO())
	db.QueryTable(new(anyObj)).Delete()

	obj := &anyObj{
		ID:      1,
		ObjOmit: obj{"", 0},
		Obj:     obj{"", 0},
	}
	_, err := db.Insert(obj)
	require.NoError(t, err)
}

func TestRawQueryRows(t *testing.T) {
	db := NewOrm(context.TODO())
	db.QueryTable(new(anyObj)).Delete()

	obj := &anyObj{
		ID:  10,
		Obj: obj{"helloworld", 100},
	}
	_, err := db.Insert(obj)
	require.NoError(t, err)

	var objs []anyObj
	err = db.Raw("select obj from any_obj where id = ?", 10).QueryRows(&objs)
	require.NoError(t, err)
	require.Equal(t, 1, len(objs))
	require.Equal(t, "helloworld", objs[0].Obj.Name)
	require.Equal(t, 100, objs[0].Obj.Value)
}

func TestMain(m *testing.M) {
	RegisterDB("default", "mysql", "orm_test:orm_test@tcp(127.0.0.1:3306)/orm_test?timeout=5s&readTimeout=15s&writeTimeout=15s", 20, 100)
	RegisterModel("default", new(shardedPerson))
	RegisterModel("default", new(jsonModel))
	RegisterModel("default", new(mapJsonModel))
	RegisterModel("default", new(dynamicModel))
	RegisterModel("default", new(anyObj))
	DebugSQLBuilder = true
	os.Exit(m.Run())
}
