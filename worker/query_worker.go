package worker

import (
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
	"database/sql"
	"strings"
	"sync/atomic"
	"github.com/500px/go-utils/metrics"
)

var (
	// Size of command buffer for new workers.
	BufferSize = 20
	concurrent = int64(0)
)

type Worker struct {
	client    string
	queryChan <-chan string
	db        *sql.DB
	wg        *sync.WaitGroup
	stats     chan<- Stat
	metrics   metrics.StatsdClient
}

func NewWorker(client string, db *sql.DB, wg *sync.WaitGroup, stats chan<- Stat, metrics metrics.StatsdClient) chan<- string {
	queryChan := make(chan string, BufferSize)

	worker := Worker{
		client:    client,
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
	for query := range w.queryChan {
		query = strings.TrimSpace(query)
		if !strings.HasPrefix(strings.ToUpper(query), "SELECT") {
			logger.Debugf("[%s] Skipping non-SELECT query: %v", w.client, query)
			continue
		}

		active := atomic.AddInt64(&concurrent, 1)
		w.metrics.Gauge("query_worker.concurrent", float64(active))
		logger.Debugf("[%s] Querying: %s", w.client, query)

		timer := metrics.NewStopwatch()
		rows, err := w.db.Query(query)
		timer.Stop()

		atomic.AddInt64(&concurrent, -1)
		logger.Debugf("[%s] Queryed", w.client)
		if err != nil {
			w.metrics.Histogram("query.duration", float64(timer.Ms()), "status:error")
			w.metrics.Incr("query.count", 1, "status:error")
			logger.Warnf("error '%v' running query: %s", err, query)
			continue
		}
		w.metrics.Histogram("query.duration", float64(timer.Ms()), "status:ok")
		w.metrics.Incr("query.count", 1, "status:ok")

		if rows.Next() {
			if strings.HasPrefix(query, "SELECT  `users`.* FROM `users` WHERE `users`.`authentication_token` =") {
				stat := Stat{}
				if err = rows.Scan(&stat.userId); err == nil {
					w.stats <- stat
				}
			} else if strings.HasPrefix(query, "SELECT  `oauth_tokens`.* FROM `oauth_tokens` WHERE `oauth_tokens`.`type` IN ('Oauth2Token')") {
				var id int32
				stat := Stat{}
				if err = rows.Scan(&id, &stat.userId); err == nil {
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
