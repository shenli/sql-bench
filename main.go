// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/ngaut/pool"
)

var (
	concurrent = flag.Int("c", 50, "concurrent workers, default: 50")
	poolSize   = flag.Int("pool", 100, "connection poll size, default: 200")
	addr       = flag.String("addr", ":4000", "tidb-server addr, default: :4000")
	dbName     = flag.String("db", "test", "db name, default: test")
	user       = flag.String("u", "root", "username, default: root")
	password   = flag.String("p", "", "password, default: empty")
	logLevel   = flag.String("L", "error", "log level, default: error")
	sqlFile    = flag.String("data", "./bench.sql", "SQL data file for bench")
)

var (
	connPool = pool.NewCache("pool", *poolSize, func() interface{} {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", *user, *password, *addr, *dbName))
		if err != nil {
			log.Fatal(err)
		}
		return db
	})
	statChan chan *stat
)

func init() {
	flag.Parse()
	statChan = make(chan *stat, 10000)
}

func cleanup() {
	// Do nothing
	for i := 0; i < *poolSize; i++ {
		db := connPool.Get().(*sql.DB)
		db.Close()
	}
}

func readQuery(queryChan chan string) {
	file, err := os.Open(*sqlFile)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		file.Close()
		close(queryChan)
	}()
	cnt := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		query := scanner.Text()
		cnt++
		if cnt%1000 == 0 {
			fmt.Printf("Read %d SQL stmts\n", cnt)
		}
		queryChan <- query
	}
	fmt.Printf("Get %d queries\n", cnt)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func worker(id int, queryChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		query, ok := <-queryChan
		if !ok {
			// No more query
			fmt.Printf("Worker[%d] done!\n", id)
			return
		}
		exec(query)
	}
}

// Structure for stat result.
type stat struct {
	spend time.Duration
	succ  bool
}

func exec(sqlStmt string) error {
	sql := strings.ToLower(sqlStmt)
	isQuery := strings.HasPrefix(sql, "select")
	// Get time
	startTs := time.Now()
	err := runQuery(sqlStmt, isQuery)
	spend := time.Now().Sub(startTs)
	s := &stat{spend: spend, succ: err == nil}
	statChan <- s
	return err
}

func runQuery(sqlStmt string, isQuery bool) error {
	db := connPool.Get().(*sql.DB)
	defer connPool.Put(db)
	if isQuery {
		rows, err := db.Query(sqlStmt)
		for {
			// Get all data.
			ok := rows.Next()
			if !ok {
				break
			}
		}
		return err
	}
	_, err := db.Exec(sqlStmt)
	return err
}

func statWorker(wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		total int64
		succ  int64
		spend time.Duration
	)
	for {
		s, ok := <-statChan
		if !ok {
			break
		}
		total++
		if s.succ {
			succ++
		}
		spend += s.spend
	}
	fmt.Printf("Query: %d, Succ: %d, Faild: %d, Time: %v, AvgTime: %v\n", total, succ, total-succ, spend, spend/time.Millisecond)
}

func main() {
	// Start
	fmt.Println("Start Bench")
	log.SetLevelByString(*logLevel)
	queryChan := make(chan string, 10000)
	wg := sync.WaitGroup{}
	wgStat := sync.WaitGroup{}
	// Start N workers
	for i := 0; i < *concurrent; i++ {
		wg.Add(1)
		go worker(i, queryChan, &wg)
	}

	wgStat.Add(1)
	go statWorker(&wgStat)
	go readQuery(queryChan)
	wg.Wait()
	close(statChan)
	cleanup()
	wgStat.Wait()
	fmt.Println("Done!")
	return
}
