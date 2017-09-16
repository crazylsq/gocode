package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"  //Go提供的数据库接口驱动
	"database/sql"
	"log"
	"math"
)

//创建一个dbpoll
type DbPoll struct {
	//mysql data source name
	Dev string
	Test string
	Prod string

}

type Asm struct {
	id int
	keyword string
	itunes_id int
	country string
	timestamp int
}


//传入dataSourceName, 返回一个int类型
func GetId(conn string) int {
	//传入数据库驱动程序名称和数据源，这里只是验证参数，不创建数据库连接，要验证数据源名称是否有效，请调用Ping
	db, err := sql.Open("mysql",conn)
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	//执行一个查询，return rows，也可用于查询中的任何占位符参数
	rows ,err := db.Query("select max(id) from aso_keyword_asm_201709")
	if err != nil {
		log.Fatal(err)
	}
	db.Close()
	var id int
	//rows是查询的结构。它的游标从结果集的第一行开始。使用Next来遍历
	for rows.Next() {
		//扫描当前行中的列复制到dest指向的值，dest中的值的数量必须与Rows中的列数相同
		if err := rows.Scan(&id); err != nil {
			log.Fatal(err)
		}
		//fmt.Println(id)
	}
	return id
}

func DataHandler(conn string,id int) <-chan Asm {
	db, err := sql.Open("mysql", conn)
	if err != nil {
		log.Fatal(err)
	}
	//查找比这个Id打的行数
	rows, err := db.Query("select count(1) from  aso_keyword_asm_201709 where id > ?", id)
	if err != nil {
		log.Fatal(err)
	}
	//创建一个channel，类型为Asm
	channel := make(chan Asm)
	go func() {
		for rows.Next() {
			var count float64
			if err := rows.Scan(&count); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("sum count: %d\n", int(count))
			var step_length int = 10
			//Ceil, 传入float64类型的参数，向上取整
			sum_steps := math.Ceil(count / 10)
			fmt.Printf("step_length: %d, sum_steps: %d",step_length,int(sum_steps))
			//批次去数据库中取数据
			for i := 0; i <= 3; i++ {
				rows, err := db.Query("select * from aso_keyword_asm_201709 where id > ? limit ?,?",id, i*step_length,step_length)
				if err != nil {
					fmt.Println(err)
				}
				for rows.Next() {
					asm := Asm{}
					if err := rows.Scan(&asm.id,&asm.keyword,&asm.itunes_id,&asm.country,&asm.timestamp); err != nil {
						log.Fatal(err)
					}
					//fmt.Println(asm.id,asm.keyword,asm.itunes_id,asm.country,asm.timestamp)
					//将数据写到channel中
					channel <- asm

				}
			}

		}
		db.Close()
		close(channel)
	}()
	return channel
}

func main() {
	//数据库连接池，里面存放数据源
	Dbw := DbPoll{
		Dev: "",
		Test: "",
		Prod: "",
	}

	id := GetId(Dbw.Test)
	fmt.Println(id)
	//fmt.Println(id)
	channel := DataHandler(Dbw.Dev,id)

	//遍历channel，依次取值
	db, err := sql.Open("mysql", Dbw.Test)
	for val := range channel {
		fmt.Println(val.id)
		if err != nil {
			log.Fatal(err)
		}
		//sql := "insert into aso_keyword_asm_201709 (id,keyword,itunes_id,country,timestamp) values (?,?,?,?,?)"
		_,err := db.Exec("insert into aso_keyword_asm_201709 (id,keyword,itunes_id,country,timestamp) values (?,?,?,?,?)", val.id,val.keyword,val.itunes_id,val.country,val.timestamp)
		if err != nil {
			fmt.Println(err)
		}

	}
	db.Close()
}
