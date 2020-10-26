package main

import (
	"fmt"

	"code.local/godb/godao"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	config := map[string]string{}
	config["username"] = "root"
	config["password"] = "123456"
	config["protocol"] = "tcp"
	//config["address"] = "127.0.0.1:3306"
	config["address"] = "192.168.1.2:3306"
	config["dbname"] = "test1"
	config["collation"] = "utf8mb4_general_ci"
	dao, err := godao.NewDaoer(config)
	fmt.Println(dao, err)
	id, err := dao.Table("author").Insert(map[string]interface{}{
		"id":       3,
		"name":     "wangjunhao",
		"password": "chocolate"})
	fmt.Println("id:", id, err)

	count, err := dao.Table("author").
		Where("id=?", []interface{}{2}).
		Update(map[string]interface{}{
			"name":     "goodnews",
			"password": "sayyes",
		})
	fmt.Println("update:", count, err)
	//	dao.Table("author").Fields("t.ooo", "t.*", "*", "name as n", "password p")
	//	dao.Table("author").Join("LEFT", "user as u", "author.id = u.id and author.id > ?", []interface{}{59})

	//dao.Table("author s").LeftJoin("teacher t", "s.id = t.id", nil).Fields("s.*", "t.degree deg").Fields("s.name").OrderBy("s.id").Offset(22).All()
	/*	dao.Table("author as a").
		LeftJoin("teacher as t", "a.password <> t.id and t.id = ?", []interface{}{1}).
		Distinct().
		Fields("a.name as Nm", "t.password").
		GroupBy("a.id").
		OrderBy("t.salary", "a.password desc").
		Having("count(a.id >?)", []interface{}{1}).
		Limit(10).
		Offset(22).
		Rows()
	*/
	/*result, err := dao.Table("author as a").
	Fields("a.password as pwd ", "a.name as Nm").
	OrderBy("a.password desc").
	Limit(10).
	Offset(1).
	Rows()
	*/

	//result, err := dao.Table("teacher").Fields("count(name) as N", "birthday B").Where("id > ?", []interface{}{1}).Rows()
	//result, err := dao.Table("teacher").Fields("count(name) as N", "birthday B").Where("id > ?", []interface{}{1}).Column()
	//result, err := dao.Table("teacher").Fields("name", "birthday B").Where("id > ?", []interface{}{0}).Column()
	//result, err := dao.Table("teacher").Fields("name", "birthday B").Where("id > ?", []interface{}{0}).Value()

	result, err := dao.Table("author").Fields("*").Rows()

	fmt.Println("查询结果", result)
}
