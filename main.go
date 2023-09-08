package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

    "github.com/jackc/pgx/v4/pgxpool"
)

var wg sync.WaitGroup
var mutex sync.Mutex

type user struct{
	first_name string
	last_name  string 
	number     int
}


func psqlConnect() *pgxpool.Pool{
	
	conn, _ := pgxpool.Connect(context.Background(), "user=postgres dbname=postgres password=12 sslmode=disable")
	
	fmt.Println("Conexão bem-sucedida ao PostgreSQL")
    return conn

}

func selectData(conn *pgxpool.Pool) []*user{
	
	var us []*user

	query := "SELECT first_name, last_name, number FROM usuarios"
	
	
	rows, _ := conn.Query(context.Background(), query)
	
	defer rows.Close()
	
	for rows.Next(){
		var u user 

		if err := rows.Scan(&u.first_name, &u.last_name, &u.number); err != nil {
			log.Fatal(err)
		}
		
		us = append(us, &u)
	}
	
	return us
}

func insertWorker(conn *pgxpool.Pool, u chan *user){
	defer wg.Done()
	
	for i := range u{
		println("Inserindo ", i.first_name)
		insert := fmt.Sprintf("insert into t (first_name, last_name, number) values ('%s', '%s', '%d');", i.first_name, i.last_name, i.number)
	
		conn.Exec(context.Background(), insert)
	}
}

func main(){
	start := time.Now()
	conn := psqlConnect()
	defer conn.Close()
	
	data := selectData(conn)
	
	fmt.Println(len(data), " dados baixados.")
	
	memory := make(chan *user)
	
	go func(){
		for i := 0; i < len(data); i++{
			memory <- data[i]
		}
		close(memory)
	}()
	
	maxWorkers := 10
	
	for i := 0; i < maxWorkers;i++{
		wg.Add(1)
		//mutex.Lock()
		go insertWorker(psqlConnect(), memory)
		//mutex.Unlock()
	}
	
	
	wg.Wait()
	
	fmt.Println(time.Since(start))
}