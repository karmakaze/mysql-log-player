package main

import (
	// "database/sql"
	"fmt"
	"os"
	"runtime"
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
	// _ "github.com/go-sql-driver/mysql"
	"github.com/500px/mysql-log-player/worker"
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

	queryChan := make(chan (string))

	wg := startWorkers(queryChan, *workerCount)

	for i := 0; i < 5; i++ {
		query, err := reader.Read()
		if err != nil {
			logger.Errorf("Failed read: %s", err)
			os.Exit(1)
		}
		queryChan <- query
	}
	close(queryChan)

	wg.Wait()
}

func getReader(path string) (*QueryReader, error) {
	if path == "" {
		return NewQueryReader(os.Stdin)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %s", err)
	}

	return NewQueryReader(file)
}

func startWorkers(queryChan <-chan (string), workerCount int) sync.WaitGroup {
	wg := sync.WaitGroup{}
	workers := []*QueryWorker{}
	for i := 0; i < workerCount; i++ {
		worker := NewQueryWorker(queryChan, &wg)
		workers = append(workers, worker)
		wg.Add(1)
		go worker.Run()
	}
	return wg
}
