package worker

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/melraidin/mysql-log-player/db"
)

var (
	// Size of command buffer for new workers.
	BufferSize = 20
	concurrent = int64(0)
	NoColumnError = errors.New("no columns")
)

type Worker struct {
	client    string
	dryRun    bool
	readOnly  bool
	queryChan <-chan string
	dbConn    *client.Conn
	wg        *sync.WaitGroup
	stats     chan<- Stat
	metrics   metrics.StatsdClient
}

func NewWorker(clientId string, connectInfo db.ConnectInfo, dryRun bool, readOnly bool, wg *sync.WaitGroup, stats chan<- Stat, metrics metrics.StatsdClient) chan<- string {
	dbConn, err := client.Connect(connectInfo.Host, connectInfo.User, connectInfo.Password, connectInfo.Database)
	if err != nil {
		logger.Errorf("Error establishing DB connection: %v", err)
		return nil
	}

	err = dbConn.SetAutoCommit()
	if err != nil {
		logger.Errorf("Error setting auto-commit on DB connection: %v", err)
		return nil
	}

	queryChan := make(chan string, BufferSize)

	worker := Worker{
		client:    clientId,
		dryRun:    dryRun,
		readOnly:  readOnly,
		queryChan: queryChan,
		dbConn:    dbConn,
		wg:        wg,
		stats:     stats,
		metrics:   metrics,
	}

	wg.Add(1)
	go worker.Run()

	return queryChan
}

func (w *Worker) Run() {
	for query := range w.queryChan {
		query = strings.TrimSpace(query)
		if w.readOnly && !strings.HasPrefix(strings.ToUpper(query), "SELECT") {
			logger.Debugf("[%s] Skipping non-SELECT query: %v", w.client, query)
			continue
		}

		if w.dryRun {
			fmt.Println(query)
			continue
		}

		if strings.HasPrefix(query, "BEGIN") {
			w.dbConn.Begin()
			continue
		} else if strings.HasPrefix(query, "COMMIT") {
			w.dbConn.Commit()
			continue
		} else if strings.HasPrefix(query, "ROLLBACK") {
			w.dbConn.Rollback()
			continue
		}

		result, err := w.dbConn.Execute(query)
		if err != nil {
			logger.Debugf("error '%v' running query: %s", err, query)
			continue
		}

		if result != nil {
			if strings.HasPrefix(query, "SELECT  `users`.* FROM `users` WHERE `users`.`authentication_token` =") {
				stat := Stat{}
				if err = extractIntColumn(result, "id", &stat.userId); err != nil {
					logger.Debugf("Error extracting user 'id': %v", err)
				} else {
					w.stats <- stat
				}
			} else if strings.HasPrefix(query, "SELECT  `oauth_tokens`.* FROM `oauth_tokens` WHERE `oauth_tokens`.`type` IN ('Oauth2Token')") {
				stat := Stat{}
				if err = extractIntColumn(result, "user_id", &stat.userId); err != nil {
					logger.Debugf("Error extracting 'user_id': %v", err)
				} else {
					w.stats <- stat
				}
			} else if strings.HasPrefix(query, "SELECT  `photos`.* FROM `photos` WHERE `photos`.`id` = ") {
				stat := Stat{}
				if err = extractIntColumn(result, "id", &stat.photoId); err != nil {
					logger.Debugf("Error extracting photo 'id': %v", err)
				} else {
					w.stats <- stat
				}
			}
		}
	}
	w.wg.Done()
}

func extractIntColumn(result *mysql.Result, name string, val *int32) error {
	if len(result.FieldNames) == 0 {
		return NoColumnError
	}
	if len(result.Values) == 0 {
		return nil
	}

	for colName, i := range result.FieldNames {
		if colName == name {
			if iVal, ok := result.Values[0][i].(int32); ok {
				*val = iVal
				return nil
			} else {
				return fmt.Errorf("Column %s not convertable to int32: %v", name, result.Values[0][i])
			}
		}
	}
	return NoColumnError
}
