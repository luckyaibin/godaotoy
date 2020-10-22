package godao

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type any = interface{}

type Daoer interface {
	Exec(string)
	Table(string) Daoer
	Fields(fields ...string)
	Insert(fields map[string]interface{}) (int64, error)
	Update(fields map[string]interface{}) (int64, error)
	Delete() (int64, error)
	Where(condition string, condValues []interface{}) Daoer

	Join(joinType string, joinTable string, joinOn string, joinParams []interface{}) Daoer
	GroupBy(fields ...string) Daoer
	Having(condition string, condValues []interface{}) Daoer
}
type Dao struct {
	db            *sql.DB
	queryTable    string
	queryFields   string
	queryDistinct bool
	//update delete也用到
	queryWhereCondition  string
	queryWhereCondValues []any

	queryJoins            []string //可以连接多个join
	queryJoinsParameters  []interface{}
	queryGroupBy          string
	queryHavingCondition  string
	queryHavingParameters []interface{}

	queryOrderBy string
	queryLimit   string
	queryOffset  string
}

func (this *Dao) Table(table string) Daoer {
	this.queryTable = doAliasTableName(table)
	return this
}

func (this *Dao) clearQuery() {
	this.queryTable = ""
	this.queryFields = "*"
	this.queryDistinct = false
	this.queryWhereCondition = ""
	this.queryWhereCondValues = []interface{}{}

	this.queryJoins = []string{}
	this.queryJoinsParameters = []interface{}{}

	this.queryGroupBy = ""
	this.queryOrderBy = ""

	this.queryLimit = ""
	this.queryOffset = ""
}

func doAliasTableName(table string) string {

	tableSlice := strings.Split(table, " ")
	//别名
	if tslen := len(tableSlice); tslen > 1 {
		return fmt.Sprintf("`%s` AS `%s`", tableSlice[0], tableSlice[tslen-1])
	} else {
		return fmt.Sprintf("`%s`", tableSlice[0])
	}
}

//select * ,name,t.name,t.name n,t.name as n这几种情况
//转换成
//select *,`name`,`t`.`name`,`t`.`name` AS `n`,`t`.`name` AS `n`的格式
func (this *Dao) Fields(fields ...string) {
	fieldList := []string{}
	for _, field := range fields {
		field = strings.TrimLeft(field, " ")
		field = strings.TrimRight(field, " ")
		fieldSlice := strings.Split(field, " ")
		//xxx yyy或 xxx as yyy 都统一成 xxx as yyy
		//当然xxx可能是aaa.a这种带.的格式,或*
		if fslen := len(fieldSlice); fslen > 1 {
			part1 := fieldSlice[0]
			part1 = doAliasColumnName(part1)
			part2 := fieldSlice[fslen-1]
			field = fmt.Sprintf("%s AS `%s`",
				part1, part2)
		} else { //可能有t1.pwd这种带.的
			field = doAliasColumnName(field)
		}

		fieldList = append(fieldList, field)
	}
	this.queryFields = strings.Join(fieldList, ",")
	fmt.Println("列名:", this.queryFields)
}
func doAliasColumnName(field string) string {

	fieldSlice := strings.Split(field, ".")
	//有.
	if fslen := len(fieldSlice); fslen > 1 {
		if fieldSlice[1] == "*" {
			return fmt.Sprintf("`%s`.%s", fieldSlice[0], fieldSlice[1])
		} else {
			return fmt.Sprintf("`%s`.`%s`", fieldSlice[0], fieldSlice[1])
		}
	} else { //没有.
		if fieldSlice[0] == "*" {
			return fmt.Sprintf("%s", fieldSlice[0])
		} else {
			return fmt.Sprintf("`%s`", fieldSlice[0])
		}
	}

}

func (this *Dao) GroupBy(fields ...string) Daoer {
	fieldSlice := []string{}
	for _, field := range fields {
		fieldSlice = append(fieldSlice, doAliasColumnName(field))
	}
	this.queryGroupBy = strings.Join(fieldSlice, ",")
	return this
}

//order by field,order by field ASC,order by field desc
//order by field1,field2 asc
func (this *Dao) OrderBy(fields ...string) Daoer {
	//field, field asc ,t.field asc
	fieldSlice := []string{}
	for _, field := range fields {
		if colmnSlice := strings.Split(field, " "); len(colmnSlice) > 1 { //有空格
			fieldSlice = append(fieldSlice, doAliasColumnName(colmnSlice[0])+" "+colmnSlice[1])
		} else {
			fieldSlice = append(fieldSlice, doAliasColumnName(colmnSlice[0]))
		}
	}
	this.queryOrderBy = strings.Join(fieldSlice, ",")
	return this
}

//limit 10;limit 10 offset 0
//限制结果集数量
func (this *Dao) Limit(size int) Daoer {
	this.queryLimit = string(size)
	return this
}

//必须搭配Limit使用
func (this *Dao) Offset(offset int) Daoer {
	this.queryOffset = string(offset)
	return this
}

//select [DISINCT] col1,coln [JOIN t2  ON t2.col1 = col1]
func (this *Dao) buildSelect() string {

	query := "SELECT"
	//distinct
	if this.queryDistinct {
		query += " DISTINCT"
	}
	//字段部分,是固定的字符串
	query += " " + this.queryFields

	//表名
	query += " " + this.queryTable

	//Join
	for _, join := range this.queryJoins {
		query += " " + join
	}
	//Groupby
	if "" != this.queryGroupBy {
		query += " GROUP BY " + this.queryGroupBy
	}
	//Having
	if "" != this.queryHavingCondition {
		query += " Having" + this.queryHavingCondition
	}
	//Order By
	if "" != this.queryOrderBy {
		query += " Order By" + this.queryOrderBy
	}
	//limit
	if "" != this.queryOffset && "" != this.queryLimit {
		query += " Limit" + this.queryLimit + " offset " + this.queryOffset
	} else if "" != this.queryLimit {
		query += " Limit" + this.queryLimit
	}
	return query
}
func (this *Dao) All() {
	query := this.buildSelect()
	fmt.Println(query)
}

/* select field1,field2 from tableName where column1 = value1
SELECT Websites.id, Websites.name, access_log.count, access_log.date
FROM Websites
INNER JOIN access_log
ON Websites.id=access_log.site_id;
*/
//INNERJOIN LEFTJOIN RIGHTJOIN
//join tableA as A on tableB.field = A.field
func (this *Dao) Join(joinType string, joinTable string, joinOn string, joinParams []interface{}) Daoer {

	join := fmt.Sprintf("%s JOIN %s ON %s ", strings.ToUpper(joinType), doAliasTableName(joinTable), joinOn)
	this.queryJoins = append(this.queryJoins, join)
	this.queryJoinsParameters = append(this.queryJoinsParameters, joinParams...)
	fmt.Println("JOIN :", this.queryJoins)
	return this
}
func (this *Dao) LeftJoin(joinTable string, joinOn string, joinParams []interface{}) Daoer {
	return this.Join("LEFT", joinTable, joinOn, joinParams)
}

//结果筛选
func (this *Dao) Having(condition string, condValues []interface{}) Daoer {
	this.queryHavingCondition = condition
	this.queryHavingParameters = condValues
	return this
}

func (this *Dao) Where(condition string, condValues []interface{}) Daoer {

	this.queryWhereCondition = condition
	this.queryWhereCondValues = condValues
	return this
}

//wrap single quotes
func sq(string string) string {
	return "'" + string + "'"
}
func (this *Dao) Insert(fields map[string]interface{}) (int64, error) {
	fieldsList := []string{}
	valueList := []interface{}{}

	valueHolderList := []string{}

	for field, value := range fields {
		fieldsList = append(fieldsList, "`"+field+"`")

		valueList = append(valueList, value)
		valueHolderList = append(valueHolderList, "?")
	}

	queryFmt := "INSERT INTO %s (%s) values (%s)"
	//sql
	sql := fmt.Sprintf(queryFmt,
		this.queryTable,
		strings.Join(fieldsList,
			", "),
		strings.Join(valueHolderList,
			", "))

	fmt.Println("Exec sql:", sql)
	result, err := this.db.Exec(sql, valueList...)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, err
}

func (this *Dao) Delete() (int64, error) {
	deleteFmt := "DELETE FROM %s WHERE %s"
	sql := fmt.Sprintf(deleteFmt, this.queryTable, this.queryWhereCondition)
	fmt.Println("Exec delete sql:", sql)
	result, err := this.db.Exec(sql, this.queryWhereCondValues...)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return count, err
}

func (this *Dao) Update(fields map[string]interface{}) (int64, error) {
	updateFmt := "UPDATE %s SET %s WHERE %s"
	fieldsList := []string{}
	valueList := []interface{}{}
	for field, value := range fields {
		fieldsList = append(fieldsList, "`"+field+"`=?")
		valueList = append(valueList, value)
	}
	allValue := append(valueList, this.queryWhereCondValues...)
	sql := fmt.Sprintf(updateFmt, this.queryTable, strings.Join(fieldsList, ", "), this.queryWhereCondition)

	fmt.Println("Exec update sql:", sql)
	result, err := this.db.Exec(sql, allValue...)
	if err != nil {
		return 0, err
	}
	id, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return id, err
}

func (this *Dao) Exec(sql string) {
}

func NewDaoer(config map[string]string) (Daoer, error) {

	// [username[:password]@][protocol[(address)]]/
	// dbname[?param1=value1&...&paramN=valueN]
	//	DSN := "root:123456@tcp(192.168.1.2:3306)/test1?collation=utf8mb4_general_ci"

	dsn := ""
	//username
	if name, ok := config["username"]; ok {
		dsn += name
		if password, ok := config["password"]; ok {
			dsn += ":" + password
		}
		dsn += "@"
	}
	//protocol
	if protocol, ok := config["protocol"]; ok {
		dsn += protocol
		if address, ok := config["address"]; ok {
			dsn += "(" + address + ")"
		}
	}
	// slash
	dsn += "/"
	//dbname
	if dbname, ok := config["dbname"]; ok {
		dsn += dbname
	}
	//parameters
	parameters := []string{}
	if collation, ok := config["collation"]; ok {
		parameters = append(parameters, "collation="+collation)
	}
	if tls, ok := config["tls"]; ok {
		parameters = append(parameters, "tls="+tls)
	}
	if len(parameters) > 0 {
		para := "?" + strings.Join(parameters, "&")
		dsn += para
	}
	//sql.Open("mysql", DSN)
	fmt.Println("Final DSN string:", dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	} else {
		fmt.Println("ping ok,连接数据库成功!")
	}
	dao := &Dao{}
	dao.db = db
	return dao, err
}
