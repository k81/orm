package orm

// QuerySetter is the advanced query interface.
type QuerySetter interface {
	// WithSuffix specifies the table suffix
	WithSuffix(tableSuffix string) QuerySetter
	// Set Distinct
	// for example:
	//  o.QueryTable("policy").Filter("Groups__Group__Users__User", user).
	//    Distinct().
	//    All(&permissions)
	Distinct() QuerySetter
	// add condition expression to QuerySetter.
	// for example:
	//	filter by UserName == 'slene'
	//	qs.Filter("UserName", "slene")
	//	sql : left outer join profile on t0.id1==t1.id2 where t1.age == 28
	//	Filter("profile__Age", 28)
	// 	 // time compare
	//	qs.Filter("created", time.Now())
	Filter(string, ...interface{}) QuerySetter
	// add NOT condition to querySeter.
	// have the same usage as Filter
	Exclude(string, ...interface{}) QuerySetter
	// set condition to QuerySetter.
	// sql's where condition
	//	cond := orm.NewCondition()
	//	cond1 := cond.And("profile__isnull", false).AndNot("status__in", 1).Or("profile__age__gt", 2000)
	//	//sql-> WHERE T0.`profile_id` IS NOT NULL AND NOT T0.`Status` IN (?) OR T1.`age` >  2000
	//	num, err := qs.SetCond(cond1).Count()
	SetCond(*Condition) QuerySetter
	// get condition from QuerySetter.
	// sql's where condition
	//  cond := orm.NewCondition()
	//  cond = cond.And("profile__isnull", false).AndNot("status__in", 1)
	//  qs = qs.SetCond(cond)
	//  cond = qs.GetCond()
	//  cond := cond.Or("profile__age__gt", 2000)
	//  //sql-> WHERE T0.`profile_id` IS NOT NULL AND NOT T0.`Status` IN (?) OR T1.`age` >  2000
	//  num, err := qs.SetCond(cond).Count()
	GetCond() *Condition
	// add GROUP BY expression
	// for example:
	//	qs.GroupBy("id")
	GroupBy(exprs ...string) QuerySetter
	// add ORDER expression.
	// "column" means ASC, "-column" means DESC.
	// for example:
	//	qs.OrderBy("-status")
	OrderBy(exprs ...string) QuerySetter
	// add OFFSET value
	Offset(offset int) QuerySetter
	// add LIMIT value.
	Limit(limit int) QuerySetter
	// for update
	ForUpdate() QuerySetter
	// return QuerySetter execution result number
	// for example:
	//	num, err = qs.Filter("profile__age__gt", 28).Count()
	Count() (int64, error)
	// check result empty or not after QuerySetter executed
	// the same as QuerySetter.Count > 0
	Exist() (bool, error)
	// execute update with parameters
	// for example:
	//	num, err = qs.Filter("user_name", "slene").Update(Params{
	//		"Nums": ColValue(Col_Minus, 50),
	//	}) // user slene's Nums will minus 50
	//	num, err = qs.Filter("UserName", "slene").Update(Params{
	//		"user_name": "slene2"
	//	}) // user slene's  name will change to slene2
	Update(values Params) (int64, error)
	// delete from table
	//for example:
	//	num ,err = qs.Filter("user_name__in", "testing1", "testing2").Delete()
	// 	//delete two user  who's name is testing1 or testing2
	Delete() (int64, error)
	// return a insert queryer.
	// it can be used in times.
	// example:
	// 	i,err := sq.PrepareInsert()
	// 	num, err = i.Insert(&user1) // user table will add one record user1 at once
	//	num, err = i.Insert(&user2) // user table will add one record user2 at once
	//	err = i.Close() //don't forget call Close
	PrepareInsert() (Inserter, error)
	// query all data and map to containers.
	// cols means the columns when querying.
	// for example:
	//	var users []*User
	//	qs.All(&users) // users[0],users[1],users[2] ...
	All(container interface{}, cols ...string) error
	// query one row data and map to containers.
	// cols means the columns when querying.
	// for example:
	//	var user User
	//	qs.One(&user) //user.UserName == "slene"
	One(container interface{}, cols ...string) error
}

var _ QuerySetter = new(querySetter)

// DefaultLimit the default limit batch size, if zero, then limit is disabled
var DefaultLimit = 1000

// real query struct
type querySetter struct {
	mi          *modelInfo
	cond        *Condition
	tableSuffix string
	limit       int
	offset      int
	orders      []string
	groups      []string
	distinct    bool
	forUpdate   bool
	forceMaster bool
	orm         *orm
}

// WithSuffix set the suffix of sharded table.
func (qs querySetter) WithSuffix(tableSuffix string) QuerySetter {
	qs.tableSuffix = tableSuffix
	return &qs
}

// ForceMaster force query in master node by add hint `{"router":"m"}`
func (qs querySetter) ForceMaster() QuerySetter {
	qs.forceMaster = true
	return &qs
}

// Distinct add "DISTINCT" in SELECT
func (qs querySetter) Distinct() QuerySetter {
	qs.distinct = true
	return &qs
}

// Filter add condition expression to QuerySetter.
func (qs querySetter) Filter(expr string, args ...interface{}) QuerySetter {
	if qs.cond == nil {
		qs.cond = NewCondition()
	}
	qs.cond = qs.cond.And(expr, args...)
	return &qs
}

// Exclude add NOT condition to querySeter.
func (qs querySetter) Exclude(expr string, args ...interface{}) QuerySetter {
	if qs.cond == nil {
		qs.cond = NewCondition()
	}
	qs.cond = qs.cond.AndNot(expr, args...)
	return &qs
}

// OrderBy add ORDER expression.
// "column" means ASC, "-column" means DESC.
func (qs querySetter) OrderBy(exprs ...string) QuerySetter {
	qs.orders = exprs
	return &qs
}

// GroupBy add GROUP expression
func (qs querySetter) GroupBy(exprs ...string) QuerySetter {
	qs.groups = exprs
	return &qs
}

// Offset add OFFSET value
func (qs querySetter) Offset(offset int) QuerySetter {
	qs.offset = offset
	return &qs
}

// Limit add LIMIT value.
// args[0] means offset, e.g. LIMIT offset, rowcount
func (qs querySetter) Limit(rowCount int) QuerySetter {
	qs.limit = rowCount
	return &qs
}

// ForUpdate add "FOR UPDATE" in SELECT
func (qs querySetter) ForUpdate() QuerySetter {
	qs.forUpdate = true
	return &qs
}

// SetCond replace current condition with cond
func (qs querySetter) SetCond(cond *Condition) QuerySetter {
	qs.cond = cond
	return &qs
}

// GetCond return current condition
func (qs *querySetter) GetCond() *Condition {
	return qs.cond
}

// Count return QuerySetter execution result number
func (qs *querySetter) Count() (int64, error) {
	return qs.mi.Count(qs.orm.ctx, qs.orm.db, qs, qs.cond)
}

// Exist check result empty or not after QuerySetter executed
func (qs *querySetter) Exist() (bool, error) {
	cnt, err := qs.mi.Count(qs.orm.ctx, qs.orm.db, qs, qs.cond)
	return cnt > 0, err
}

// Update execute update with parameters
func (qs *querySetter) Update(params Params) (int64, error) {
	return qs.mi.UpdateBatch(qs.orm.ctx, qs.orm.db, qs, qs.cond, params)
}

// Delete execute delete
func (qs *querySetter) Delete() (int64, error) {
	return qs.mi.DeleteBatch(qs.orm.ctx, qs.orm.db, qs, qs.cond)
}

// return a insert queryer.
// it can be used in times.
// example:
// 	 i,err := sq.PrepareInsert()
// 	 num, err = i.Insert(&user1) // user table will add one record user1 at once
//	 num, err = i.Insert(&user2) // user table will add one record user2 at once
//	 err = i.Close() //don't forget call Close
func (qs *querySetter) PrepareInsert() (Inserter, error) {
	return newPreparedInserter(qs.orm, qs.mi, qs.tableSuffix)
}

// All query all data and map to containers.
// cols means the columns when querying.
func (qs *querySetter) All(container interface{}, cols ...string) error {
	if qs.limit == 0 && DefaultLimit != 0 {
		qs.limit = DefaultLimit
	}
	return qs.mi.ReadBatch(qs.orm.ctx, qs.orm.db, qs, qs.cond, container, cols)
}

// One query one row data and map to containers.
// cols means the columns when querying.
func (qs *querySetter) One(container interface{}, cols ...string) error {
	qs.limit = 1
	return qs.mi.ReadOne(qs.orm.ctx, qs.orm.db, qs, qs.cond, container, cols)
}

// create new QuerySetter.
func newQuerySetter(orm *orm, mi *modelInfo) QuerySetter {
	if !orm.isTx {
		orm.Using(mi.db)
	}

	qs := &querySetter{
		orm: orm,
		mi:  mi,
	}
	return qs
}
