package main

import (
	"code.local/godb/godao"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
)

func main() {
	config := map[string]string{}
	config["username"] = "root"
	config["password"] = "123456"
	config["protocol"] = "tcp"
	config["address"] = "127.0.0.1:3306"
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
	dao.Table("author").Fields("t.ooo", "t.*", "*", "name as n", "password p")
}

func main0() {
	DSN := "root:123456@tcp(192.168.1.2:3306)/test1?collation=utf8mb4_general_ci"
	//open db
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		fmt.Println(err)
		return
	}
	//fmt.Printf("%+v", db)
	err = db.Ping()
	if err != nil {
		fmt.Println("ping error", err)
	}

	//	tx, err := db.Begin()
	sql := "insert into `author`(`id`,`name`,`password`) values(?,?,?)"
	multiline := `ggghsj
	hhhrje
	`
	fmt.Println(multiline)
	//	sql := "select `name`,`password` from author"
	result, err := db.Exec(sql, 5, "wanger", "1234567")
	//	result, err := db.Query(sql)
	fmt.Println(reflect.TypeOf(result))
	if err != nil {
		fmt.Println("insert :", err)
	}
	//	tx.Exec(sql, 1, "wang", "1234")
	//	err = tx.Commit()
	//	fmt.Println("commit:", err)
}
