package main

import (
	"database/sql"
	"fmt"
	"log"
	"io"
	"os"
	"runtime"
	_ "github.com/ziutek/mymysql/godrv"
	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/melraidin/mysql-log-player/query"
	"github.com/melraidin/mysql-log-player/worker"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	parseFlags()

	reader, err := getReader(*sourcePath)
	if err != nil {
		logger.Errorf("Failed initialization: %s", err)
		os.Exit(1)
	}
	defer reader.Close()
	//
	connectInfo := fmt.Sprintf("tcp:%s:3306*%s/%s/%s", *dbHost, *dbName, *dbUser, *dbPass)
	fmt.Printf("connection info: %v\n", connectInfo)
	db, err := sql.Open("mymysql", connectInfo)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	err = db.Ping()
	if err != nil {
		logger.Errorf("Error pinging db: %v", err)
		os.Exit(1)
	}

	queryPool := worker.NewWorkerPool(db)

	var query *query.Query
	for query, err = reader.Read(); err == nil; query, err = reader.Read() {
		queryPool.Dispatch(query)
	}

	if err != nil && err != io.EOF {
		logger.Errorf("Failed read: %s", err)
	}

	queryPool.Wait()
}

func getReader(path string) (*query.Reader, error) {
	if path == "" {
		return query.NewReader(os.Stdin)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %s", err)
	}

	return query.NewReader(file)
}
