package worker

import (
	"strings"
	"sync"
	"sync/atomic"
	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/melraidin/mysql-log-player/conn"
)

var (
	// Size of command buffer for new workers.
	BufferSize = 50
	concurrent = int64(0)
)

type Worker struct {
	client      string
	queryChan   <-chan string
	db          conn.DBConn
	concurrency *Concurrency
	wg          *sync.WaitGroup
	stats       chan<- Stat
	metrics     metrics.StatsdClient
}

func NewWorker(client string, dbOpener conn.DBOpener, concurrency *Concurrency, wg *sync.WaitGroup, stats chan<- Stat, metrics metrics.StatsdClient) chan<- string {
	queryChan := make(chan string, BufferSize)

	db, err := dbOpener.Open()
	if err != nil {
		logger.Errorf("[%s] error from dbOpener: %v", client, err)
		return nil
	}

	worker := Worker{
		client:      client,
		queryChan:   queryChan,
		db:          db,
		concurrency: concurrency,
		wg:          wg,
		stats:       stats,
		metrics:     metrics,
	}

	wg.Add(1)
	go worker.Run()

	return queryChan
}

func (w *Worker) Run() {
	for query := range w.queryChan {
		w.metrics.Gauge("query.chan", float64(len(query)), "client:"+w.client)
		//query = strings.TrimSpace(query)
		//if !strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		//	//logger.Debugf("[%s] Skipping non-SELECT query: %v", query)
		//	continue
		//}
		_ = w.doQuery(query)
	}
	w.wg.Done()
}

func (w *Worker) doQuery(query string) error {
	//logger.Debugf("[%s] Querying: %s", w.client, query)
	timer := metrics.NewStopwatch()
	active := atomic.AddInt64(&concurrent, 1)

	rows, err := w.db.Query(query)

	atomic.AddInt64(&concurrent, -1)
	timer.Stop()
	w.metrics.Gauge("query_worker.concurrent", float64(active))

	//logger.Debugf("[%s] Queryed", w.client)
	if err != nil {
		w.metrics.Histogram("query.duration", float64(timer.Ms()), "status:error")
		w.metrics.Incr("query.count", 1, "status:error", "client:"+w.client)
		//logger.Debugf("[%s] error '%v' running query: %s", w.client, err, query)
		return err
	}
	w.metrics.Histogram("query.duration", float64(timer.Ms()), "status:ok")
	w.metrics.Incr("query.count", 1, "status:ok", "client:"+w.client)

	defer func() {
		if err := rows.Close(); err != nil {
			logger.Errorf("[%s] error closing rows: %v", w.client, err)
		}
	}()

	row, err := rows.First()
	if err != nil {
		//logger.Warnf("[%s] Error '%v' in rows.First: %s", w.client, err, query)
		return err
	}

	if strings.HasPrefix(query, "SELECT  `users`.* FROM `users` WHERE `users`.`authentication_token` =") {
		stat := Stat{}
		if stat.userId, err = row.Int("id"); err != nil {
			logger.Warnf("[%s] Error extracting user 'id': %v", w.client, err)
		} else {
			w.stats <- stat
		}
	} else if strings.HasPrefix(query, "SELECT  `oauth_tokens`.* FROM `oauth_tokens` WHERE `oauth_tokens`.`type` IN ('Oauth2Token')") {
		stat := Stat{}
		if stat.userId, err = row.Int("user_id"); err != nil {
			logger.Warnf("[%s] Error extracting 'user_id': %v", w.client, err)
		} else {
			w.stats <- stat
		}
	} else if strings.HasPrefix(query, "SELECT  `photos`.* FROM `photos` WHERE `photos`.`id` = ") {
		stat := Stat{}
		if stat.photoId, err = row.Int("id"); err != nil {
			logger.Warnf("[%s] Error extracting photo 'id': %v", w.client, err)
		} else {
			w.stats <- stat
		}
	}
	return nil
}
