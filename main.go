package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"
	//_ "github.com/ziutek/mymysql/godrv"
	_ "github.com/go-sql-driver/mysql"
	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/melraidin/mysql-log-player/conn"
	"github.com/melraidin/mysql-log-player/query"
	"github.com/melraidin/mysql-log-player/worker"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 20)

	parseFlags()

	startDebugListener()

	statsdClient, err := metrics.NewStatsdClient("mysql_log_player", "", "127.0.0.1:8125")
	exitOnError(err)
	logger.Debugf("Created statsd client.")
	statsdClient.Incr("start", 1)

	logger.Debugf("Creating query pool:")

	//hostPort := fmt.Sprintf("%s:3306", *dbHost)
	//dbOpener := conn.NewMyMySQLOpener(hostPort, *dbUser, *dbPass, *dbName)

	connectInfo := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?allowOldPasswords=1", *dbUser, *dbPass, *dbHost, *dbName)
	dbOpener := conn.NewGoMySQLOpener("mysql", connectInfo)

	//dbOpener := conn.NewSiddontangOpener(hostPort, *dbUser, *dbPass, *dbName)

	queryPool := worker.NewWorkerPool(dbOpener, *dbConcurrency, statsdClient)
	logger.Debugf("Created query pool.")

	time.Sleep(5 * time.Second)
	logger.Infof("Dispatching queries...")

	i := 0
	err := foreach(func() {
		i += 1
		//logger.Debugf("Dispatching query: %d", i)
		queryPool.Dispatch(query)
		//logger.Debugf("Dispatched query: %d", i)
	})
	if err != nil {

	}

	for query, err = reader.Read(); err == nil; query, err = reader.Read() {
	}

	if err != nil && err != io.EOF {
		logger.Errorf("Failed read: %s", err)
	}

	queryPool.Wait()
}

func foreach(f func(q *query.Query)) error {
	reader, err := getReader(*sourcePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	var query *query.Query
	for query, err = reader.Read(); err == nil; query, err = reader.Read() {
		f(query)
	}

	return nil
}

func startDebugListener() {
	go func() {
 		log.Println(http.ListenAndServe("localhost:6060", nil))
 	}()
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

func exitOnError(err error) {
	if err != nil {
		logger.Errorf("Error pinging db: %v", err)
		os.Exit(1)
	}
}
