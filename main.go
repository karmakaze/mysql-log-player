package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/melraidin/mysql-log-player/query"
	"github.com/melraidin/mysql-log-player/worker"
	"github.com/melraidin/mysql-log-player/db"
)

func main() {
	if runtime.GOMAXPROCS(0) == 1 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	parseFlags()

	logger.Infof("format    = %v", *format)
	logger.Infof("read-only = %v", *readOnly)
	logger.Infof("dry-run   = %v", *dryRun)
	//logger.Infof("workers = ", *workerCount)

	reader, err := getReader(*sourcePath)
	if err != nil {
		logger.Errorf("Failed initialization: %s", err)
		os.Exit(1)
	}
	defer reader.Close()

	statsdClient, err := metrics.NewStatsdClient("mysql_log_player", "", "127.0.0.1:8125")
	exitOnError(err)
	logger.Debugf("Created statsd client.")
	statsdClient.Incr("start", 1)

	connectInfo := db.ConnectInfo{
		Host: *dbHost,
		User: *dbUser,
		Password: *dbPass,
		Database: *dbName,
	}

	logger.Debugf("Creating query pool:")
	queryPool := worker.NewWorkerPool(connectInfo, *dryRun, *readOnly, statsdClient)
	logger.Debugf("Created query pool.")

	logger.Debugf("Dispatching queries...")

	i := 0
	var query *query.Query
	for query, err = reader.Read(); err == nil; query, err = reader.Read() {
		i += 1
		logger.Debugf("Dispatching query: %d", i)
		queryPool.Dispatch(query)
		logger.Debugf("Dispatched query: %d", i)
	}

	if err != nil && err != io.EOF {
		logger.Errorf("Failed read: %s", err)
	}

	queryPool.Wait()
}

func getReader(path string) (*query.Reader, error) {
	var qFormat query.Format
	if *format == "mysql-sniffer" {
		qFormat = query.FORMAT_MYSQL_SNIFFER
	} else if *format == "vc-mysql-sniffer" {
		qFormat = query.FORMAT_VC_MYSQL_SNIFFER
	}

	if path == "" {
		logger.Debugf("Reading from stdin")
		return query.NewReader(qFormat, os.Stdin)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %s", err)
	}

	return query.NewReader(qFormat, file)
}

func exitOnError(err error) {
	if err != nil {
		logger.Errorf("Error pinging db: %v", err)
		os.Exit(1)
	}
}
