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
	BufferSize = 200
	concurrent = int64(0)
	NoColumnError = errors.New("no columns")
)

type Worker struct {
	client      string
	dryRun      bool
	readOnly    bool
	updatesOnly bool
	queryChan   <-chan string
	dbConn      mysql.Conn
	wg          *sync.WaitGroup
	metrics     metrics.StatsdClient
}

func NewWorker(clientId string, connectInfo db.ConnectInfo, dryRun bool, readOnly bool, updatesOnly bool, wg *sync.WaitGroup, metrics metrics.StatsdClient) chan<- string {
	dbConn := mysql.New("tcp", "", connectInfo.Host, connectInfo.User, connectInfo.Password, connectInfo.Database)
	err := dbConn.Connect()
	if err != nil {
		logger.Errorf("Error establishing DB connection: %v", err)
		return nil
	}

	_, _, err = dbConn.Query("SET autocommit=1;")
	if err != nil {
		logger.Errorf("Error setting autocommit on DB connection: %v", err)
		return nil
	}

	queryChan := make(chan string, BufferSize)

	worker := Worker{
		client:      clientId,
		dryRun:      dryRun,
		readOnly:    readOnly,
		updatesOnly: updatesOnly,
		queryChan:   queryChan,
		dbConn:      dbConn,
		wg:          wg,
		metrics:     metrics,
	}

	wg.Add(1)
	go worker.Run()

	return queryChan
}

func (w *Worker) Run() {
	var tx mysql.Transaction

	//fmt.Printf("[%s] read-only=%v updates-only=%v dry-run=%v\n", w.client, w.readOnly, w.updatesOnly, w.dryRun)

	for query := range w.queryChan {
		query = strings.TrimSpace(query)

		if w.readOnly && !strings.HasPrefix(strings.ToUpper(query), "SELECT") &&
		                 !strings.HasPrefix(strings.ToUpper(query), "(SELECT") {
			logger.Debugf("[%s] Skipping non-SELECT query: %v\n", w.client, query)
			continue
		}
		if !w.readOnly && !strings.HasPrefix(strings.ToUpper(query), "UPDATE") {
			logger.Debugf("[%s] Skipping non-UPDATE query: %v\n", w.client, query)
			continue
		}

		if w.dryRun {
			fmt.Println(query)
			continue
		}

		if strings.HasPrefix(query, "BEGIN") {
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
		} else if strings.HasPrefix(query, "COMMIT") {
			if tx != nil {
				tx.Commit()
				tx = nil
			}
			continue
		} else if strings.HasPrefix(query, "ROLLBACK") {
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
				}
			} else if strings.HasPrefix(query, "SELECT  `oauth_tokens`.* FROM `oauth_tokens` WHERE `oauth_tokens`.`type` IN ('Oauth2Token')") {
				stat := Stat{}
				if err = extractIntColumn(rows[0], result, "user_id", &stat.userId); err != nil {
					logger.Debugf("Error extracting 'user_id': %v", err)
				}
			} else if strings.HasPrefix(query, "SELECT  `photos`.* FROM `photos` WHERE `photos`.`id` = ") {
				stat := Stat{}
				if err = extractIntColumn(rows[0], result, "id", &stat.photoId); err != nil {
					logger.Debugf("Error extracting photo 'id': %v", err)
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
