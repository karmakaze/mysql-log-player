package main

import (
	// "database/sql"
	"fmt"
	"io"
	"os"
	"runtime"

	logger "github.com/500px/go-utils/chatty_logger"
	// _ "github.com/go-sql-driver/mysql"
	"github.com/Melraidin/mysql-log-player/query"
	"github.com/Melraidin/mysql-log-player/worker"
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

	queryPool := worker.NewWorkerPool()

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
