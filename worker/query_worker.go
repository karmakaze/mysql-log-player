package worker

import (
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
	"database/sql"
	"strings"
//	"sync/atomic"
	"github.com/500px/go-utils/metrics"
	"errors"
	"fmt"
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
	db        *sql.DB
	wg        *sync.WaitGroup
	stats     chan<- Stat
	metrics   metrics.StatsdClient
}

func NewWorker(client string, db *sql.DB, dryRun bool, readOnly bool, wg *sync.WaitGroup, stats chan<- Stat, metrics metrics.StatsdClient) chan<- string {
	queryChan := make(chan string, BufferSize)

	worker := Worker{
		client:    client,
		dryRun:    dryRun,
		readOnly:  readOnly,
		queryChan: queryChan,
		db:        db,
		wg:        wg,
		stats:     stats,
		metrics:   metrics,
	}

	wg.Add(1)
	go worker.Run()

	return queryChan
}

func (w *Worker) Run() {
	var tx *sql.Tx

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
			tx, err = w.db.Begin()
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

		rows, err := w.db.Query(query)
		if err != nil {
			logger.Debugf("error '%v' running query: %s", err, query)
			continue
		}

		if rows.Next() {
			if strings.HasPrefix(query, "SELECT  `users`.* FROM `users` WHERE `users`.`authentication_token` =") {
				stat := Stat{}
				if err = extractIntColumn(rows, "id", &stat.userId); err != nil {
					logger.Debugf("Error extracting user 'id': %v", err)
				} else {
					w.stats <- stat
				}
			} else if strings.HasPrefix(query, "SELECT  `oauth_tokens`.* FROM `oauth_tokens` WHERE `oauth_tokens`.`type` IN ('Oauth2Token')") {
				stat := Stat{}
				if err = extractIntColumn(rows, "user_id", &stat.userId); err != nil {
					logger.Debugf("Error extracting 'user_id': %v", err)
				} else {
					w.stats <- stat
				}
			} else if strings.HasPrefix(query, "SELECT  `photos`.* FROM `photos` WHERE `photos`.`id` = ") {
				stat := Stat{}
				if err = extractIntColumn(rows, "id", &stat.photoId); err != nil {
					logger.Debugf("Error extracting photo 'id': %v", err)
				} else {
					w.stats <- stat
				}
			}
		}
		err = rows.Close()
		if err != nil {
			logger.Warnf("error closing rows: %v", err)
		}
	}
	w.wg.Done()
}

func extractIntColumn(rows *sql.Rows, name string, val *int32) error {
	colNames, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(colNames) == 0 {
		return NoColumnError
	}

	values := make([]interface{}, len(colNames))
	for i, colName := range colNames {
		if colName == name {
			values[i] = val
		} else {
			var iface interface{}
			values[i] = &iface
		}
	}
	if err = rows.Scan(values...); err != nil {
		return err
	}
	return nil
}
