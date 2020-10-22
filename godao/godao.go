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
	Where(condition string, condValues []interface{}) Daoer
	Delete() (int64, error)
	Join(joinType string, joinTable string, joinOn string, joinParams []interface{}) Daoer
}
type Dao struct {
	db              *sql.DB
	queryTable      string
	queryFields     string
	queryDistinct   bool
	queryCondition  string
	queryCondValues []any

	queryJoins           []string //可以连接多个join
	queryJoinsParameters []interface{}
}

func (this *Dao) Table(table string) Daoer {
	this.queryTable = doAliasTableName(table)
	return this
}

func (this *Dao) clearQuery() {
	this.queryTable = ""
	this.queryFields = "*"
	this.queryDistinct = false
	this.queryCondition = ""
	this.queryCondValues = []interface{}{}

	this.queryJoins = []string{}
	this.queryJoinsParameters = []interface{}{}
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

func (this *Dao) GroupBy(fields ...string) {

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
func (this *Dao) Delete() (int64, error) {
	deleteFmt := "DELETE FROM %s WHERE %s"
	sql := fmt.Sprintf(deleteFmt, this.queryTable, this.queryCondition)
	fmt.Println("Exec delete sql:", sql)
	result, err := this.db.Exec(sql, this.queryCondValues...)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return count, err
}

func (this *Dao) Where(condition string, condValues []interface{}) Daoer {

	this.queryCondition = condition
	this.queryCondValues = condValues
	return this
}

//wrap single quotes
func sq(string string) string {
	return "'" + string + "'"
}

func (this *Dao) Update(fields map[string]interface{}) (int64, error) {
	updateFmt := "UPDATE %s SET %s WHERE %s"
	fieldsList := []string{}
	valueList := []interface{}{}
	for field, value := range fields {
		fieldsList = append(fieldsList, "`"+field+"`=?")
		valueList = append(valueList, value)
	}
	allValue := append(valueList, this.queryCondValues...)
	sql := fmt.Sprintf(updateFmt, this.queryTable, strings.Join(fieldsList, ", "), this.queryCondition)

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
