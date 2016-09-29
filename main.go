package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"runtime"
	//_ "github.com/ziutek/mymysql/godrv"
	_ "github.com/go-sql-driver/mysql"
	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
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

	statsdClient, err := metrics.NewStatsdClient("mysql_log_player", "", "127.0.0.1:8125")
	exitOnError(err)
	logger.Debugf("Created statsd client.")
	statsdClient.Incr("start", 1)

	var db *sql.DB
	if !*dryRun {
		//
		connectInfo := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?allowOldPasswords=1", *dbUser, *dbPass, *dbHost, *dbName)
		db, err = sql.Open("mysql", connectInfo)
		exitOnError(err)

		db.SetMaxOpenConns(900)
		db.SetMaxIdleConns(10000) // fix TIME_WAIT with Go-MySQL-Driver https://www.percona.com/blog/2014/05/14/tips-benchmarking-go-mysql/

		err = db.Ping()
		exitOnError(err)

		logger.Debugf("db.Ping OK")

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
		exitOnError(err)
		logger.Debugf("COUNT(*) users: %d", count)
	}

	logger.Debugf("Creating query pool:")
	queryPool := worker.NewWorkerPool(db, *dryRun, *readOnly, statsdClient)
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
	if path == "" {
		logger.Debugf("Reading from stdin")
		return query.NewReader(os.Stdin)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %s", err)
	}

	return query.NewReader(file)
}

func exitOnError(err error) {
	if err != nil {
		logger.Errorf("Error pinging db: %v", err)
		os.Exit(1)
	}
}
