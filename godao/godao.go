package godao

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Daoer interface {
	Table(string) Daoer
	Distinct() Daoer
	Fields(fields ...interface{}) Daoer
	Where(condition string, condValues []interface{}) Daoer
	Join(joinType string, joinTable string, joinOn string, joinParams []interface{}) Daoer
	LeftJoin(joinTable string, joinOn string, joinParams []interface{}) Daoer
	GroupBy(fields ...string) Daoer
	OrderBy(fields ...string) Daoer
	Having(condition string, condValues []interface{}) Daoer
	Limit(size int) Daoer
	Offset(offset int) Daoer

	//Insert 插入数据
	Insert(fields map[string]interface{}) (int64, error)
	//Update 修改数据
	Update(fields map[string]interface{}) (int64, error)
	//Delete 删除数据
	Delete() (int64, error)

	//Rows 查询出多行数据
	Rows() ([]map[string]string, error)
	//Row 查询出一行数据
	Row() (map[string]string, error)
	//Column 查询出第一列的数据
	Column() ([]string, error)
	//Value 查询出第一行第一列的数据
	Value() (string, error)
}
type Dao struct {
	db         *sql.DB
	queryTable string
	//insert，select也用到queryFields
	queryFields   string
	queryDistinct bool
	//update delete也用到
	queryWhereCondition  string
	queryWhereCondValues []interface{}

	queryJoins           []string //可以连接多个join
	queryJoinsParameters []interface{}

	queryHavingCondition  string
	queryHavingParameters []interface{}

	queryGroupBy string
	queryOrderBy string
	queryLimit   string
	queryOffset  string
}

/*NewDaoer 创建一个DAO
参数实例:
	config["username"] = "root"
	config["password"] = "123456"
	config["protocol"] = "tcp"
	//config["address"] = "127.0.0.1:3306"
	config["address"] = "192.168.1.2:3306"
	config["dbname"] = "test1"
	config["collation"] = "utf8mb4_general_ci"
*/
func NewDaoer(config map[string]string) (Daoer, error) {

	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
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

	this.queryHavingCondition = ""
	this.queryHavingParameters = []interface{}{}

	this.queryGroupBy = ""
	this.queryOrderBy = ""

	this.queryLimit = ""
	this.queryOffset = ""
}

func (this *Dao) Distinct() Daoer {
	this.queryDistinct = true
	return this
}

//select * ,name,t.name,t.name n,t.name as n这几种情况
//转换成
//select *,`name`,`t`.`name`,`t`.`name` AS `n`,`t`.`name` AS `n`的格式
func (this *Dao) Fields(fields ...interface{}) Daoer {
	fieldList := []string{}
	for _, field := range fields {
		switch field.(type) {
		case string:
			fieldstr := field.(string)
			fieldstr = strings.TrimLeft(fieldstr, " ")
			fieldstr = strings.TrimRight(fieldstr, " ")
			fieldSlice := strings.Split(fieldstr, " ")
			//xxx yyy或 xxx as yyy 都统一成 xxx as yyy
			//当然xxx可能是aaa.a这种带.的格式,或*
			if fslen := len(fieldSlice); fslen > 1 {
				part1 := fieldSlice[0]
				part1 = doAliasColumnName(part1)
				part2 := fieldSlice[fslen-1]
				field = fmt.Sprintf("%s AS `%s`",
					part1, part2)
			} else { //可能有t1.pwd这种带.的
				fieldstr = doAliasColumnName(fieldstr)
			}

			fieldList = append(fieldList, fieldstr)
		case RawString:
			fieldList = append(fieldList, field.(RawString).String)
		}

	}
	this.queryFields = strings.Join(fieldList, ",")
	//fmt.Println("列名:", this.queryFields)
	return this
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
	defer this.clearQuery()
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
	defer this.clearQuery()
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
	defer this.clearQuery()
	if err != nil {
		return 0, err
	}
	id, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return id, err
}

type RawString struct {
	String string
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
	this.queryLimit = strconv.Itoa(size)
	return this
}

//必须搭配Limit使用
func (this *Dao) Offset(offset int) Daoer {
	this.queryOffset = strconv.Itoa(offset)
	return this
}

//返回第一行第一列的值
func (this *Dao) Value() (string, error) {
	row, cols, err := this.rowColumns()
	if err != nil {
		return "", err
	}
	if len(row) > 0 {
		firstColName := cols[0]
		return row[firstColName], nil
	}
	return "", nil
}

//返回第一列
func (this *Dao) Column() ([]string, error) {
	rows, cols, err := this.rowsColumns()
	if err != nil {
		return []string{}, err
	}
	firstColName := cols[0]
	result := []string{}
	for _, row := range rows {
		result = append(result, row[firstColName])
	}
	return result, err
}

//返回第1行
func (this *Dao) Row() (map[string]string, error) {
	this.Limit(1)
	row, _, err := this.rowColumns()
	if err != nil {
		return map[string]string{}, err
	}
	if len(row) > 0 {
		return row, nil
	} else {
		return map[string]string{}, nil
	}
}

//返回多列
func (this *Dao) Rows() ([]map[string]string, error) {
	result, _, err := this.rowsColumns()
	return result, err
}

//返回一行和列名
func (this *Dao) rowColumns() (map[string]string, []string, error) {
	this.Limit(1)
	rows, cols, err := this.rowsColumns()
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	//是否有数据
	if len(rows) > 0 {
		return rows[0], cols, nil
	} else {
		return map[string]string{}, cols, nil
	}
}

//返回多列和列名
func (this *Dao) rowsColumns() ([]map[string]string, []string, error) {
	//1. 构建sql
	query := this.buildSelect()
	//2. 执行，传入参数
	//注意传入的参数的顺序，[SELECT t1.name from t1 LEFT JOIN t2 on xxx WHERE yyy GROUP BY t1.age HAVING zzz
	//xxx,yyy,zzz分别是queryJoinsParameters,queryWhereCondValues,queryHavingParameters
	allParams := append(this.queryJoinsParameters, append(this.queryWhereCondValues, this.queryHavingParameters...)...)
	defer this.clearQuery()
	rows, err := this.db.Query(query, allParams...)
	if err != nil {
		fmt.Print("Rows() Query执行出错", err)
		return []map[string]string{}, []string{}, err
	}
	defer rows.Close()

	//确定列的数量
	columns, err := rows.Columns()
	if err != nil {
		fmt.Print("Columns执行出错", err)
		return []map[string]string{}, columns, err
	}
	colNum := len(columns)
	oneRowValues := make([]sql.NullString, colNum)
	oneRowInterfaces := make([]interface{}, colNum)
	for i, _ := range oneRowInterfaces {
		oneRowInterfaces[i] = &oneRowValues[i]
	}
	result := []map[string]string{}
	//3.处理结果
	for rows.Next() {
		//获取一行
		scanErr := rows.Scan(oneRowInterfaces...)
		if scanErr != nil {
			fmt.Print("Rows() Scan执行出错", scanErr)
			continue //null可能获取不到
		}
		row := map[string]string{}
		for i, _ := range oneRowInterfaces {
			name := columns[i]
			ns := *(oneRowInterfaces[i].(*sql.NullString))
			if ns.Valid {
				row[name] = ns.String
			} else {
				row[name] = "" //NULL
			}
		}
		result = append(result, row)
	}
	return result, columns, nil
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
	return this
}

func (this *Dao) LeftJoin(joinTable string, joinOn string, joinParams []interface{}) Daoer {
	return this.Join("LEFT", joinTable, joinOn, joinParams)
}

func (this *Dao) RightJoin(joinTable string, joinOn string, joinParams []interface{}) Daoer {
	return this.Join("RIGHT", joinTable, joinOn, joinParams)
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

/*
//查询语句范例
SELECT DISTINCT a.name,t.name as t_name
From author AS a
LEFT JOIN teacher as t ON a.id > 0 and a.name = b.name
WHERE a.id > 1
GROUP BY a.age
HAVING a.age>30
ORDER BY a.salary ASC;

//ON是针对JOIN两个表操作的前提条件
//WHERE是针对单个表或者JOIN之后产生的临时表进行筛选
//HAVING针对的是GROUP BY之后的结果进行过滤的
*/
func (this *Dao) buildSelect() string {

	query := "SELECT"
	//distinct
	if this.queryDistinct {
		query += " DISTINCT"
	}
	//字段部分,是固定的字符串
	query += " " + this.queryFields

	//表名
	query += " FROM" + this.queryTable

	//Join
	for _, join := range this.queryJoins {
		query += " " + join
	}

	if "" != this.queryWhereCondition {
		query += " WHERE " + this.queryWhereCondition
	}
	//Groupby
	if "" != this.queryGroupBy {
		query += " GROUP BY " + this.queryGroupBy
	}
	//Having
	if "" != this.queryHavingCondition {
		query += " HAVING " + this.queryHavingCondition
	}
	//Order By
	if "" != this.queryOrderBy {
		query += " ORDER BY " + this.queryOrderBy
	}
	//limit
	if "" != this.queryOffset && "" != this.queryLimit {
		query += " LIMIT " + this.queryLimit + " OFFSET " + this.queryOffset
	} else if "" != this.queryLimit {
		query += " LIMIT " + this.queryLimit
	}
	return query
}
