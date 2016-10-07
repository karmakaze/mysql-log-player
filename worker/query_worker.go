package worker

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
    "github.com/ziutek/mymysql/mysql"
    _ "github.com/ziutek/mymysql/native" // Native engine
    // _ "github.com/ziutek/mymysql/thrsafe" // Thread safe engine
	// 	"github.com/siddontang/go-mysql/client"
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
	dbConn    mysql.Conn
	wg        *sync.WaitGroup
	stats     chan<- Stat
	metrics   metrics.StatsdClient
}

func NewWorker(clientId string, connectInfo db.ConnectInfo, dryRun bool, readOnly bool, wg *sync.WaitGroup, stats chan<- Stat, metrics metrics.StatsdClient) chan<- string {
	dbConn := mysql.New("tcp", "", connectInfo.Host, connectInfo.User, connectInfo.Password, connectInfo.Database)
	err := dbConn.Connect()
	if err != nil {
		logger.Errorf("Error establishing DB connection: %v", err)
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
	var tx mysql.Transaction
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

		queryPrefix := strings.TrimLeft(query, " \t\n")
		if strings.HasPrefix(queryPrefix, "BEGIN") {
			if tx != nil {
				tx.Rollback()
				tx = nil
			}
			var err error
			tx, err = w.dbConn.Begin()
			if err != nil {
				logger.Errorf("Error beginning transaction: %v", err)
			}
			continue
		} else if strings.HasPrefix(queryPrefix, "COMMIT") {
			if tx != nil {
				tx.Commit()
				tx = nil
			}
			continue
		} else if strings.HasPrefix(queryPrefix, "ROLLBACK") {
			if tx != nil {
				tx.Rollback()
				tx = nil
			}
			continue
		}

		rows, result, err := w.dbConn.Query(query)
		if err != nil {
			logger.Debugf("error '%v' running query: %s", err, query)
			continue
		}

		if len(rows) > 0 {
			if strings.HasPrefix(query, "SELECT  `users`.* FROM `users` WHERE `users`.`authentication_token` =") {
				stat := Stat{}
				if err = extractIntColumn(rows[0], result, "id", &stat.userId); err != nil {
					logger.Debugf("Error extracting user 'id': %v", err)
				} else {
					w.stats <- stat
				}
			} else if strings.HasPrefix(query, "SELECT  `oauth_tokens`.* FROM `oauth_tokens` WHERE `oauth_tokens`.`type` IN ('Oauth2Token')") {
				stat := Stat{}
				if err = extractIntColumn(rows[0], result, "user_id", &stat.userId); err != nil {
					logger.Debugf("Error extracting 'user_id': %v", err)
				} else {
					w.stats <- stat
				}
			} else if strings.HasPrefix(query, "SELECT  `photos`.* FROM `photos` WHERE `photos`.`id` = ") {
				stat := Stat{}
				if err = extractIntColumn(rows[0], result, "id", &stat.photoId); err != nil {
					logger.Debugf("Error extracting photo 'id': %v", err)
				} else {
					w.stats <- stat
				}
			}
		}
	}
	w.wg.Done()
}

func extractIntColumn(row mysql.Row, result mysql.Result, name string, val *int32) error {
	*val = int32(row.Int(result.Map(name)))
	return nil
}
