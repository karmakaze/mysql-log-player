package worker

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/melraidin/mysql-log-player/conn"
	"github.com/melraidin/mysql-log-player/query"
	"fmt"
)

var (
	// Size of command buffer for new workers.
	BufferSize = 50
	concurrent = int64(0)
)

type Worker struct {
	Logtime0    time.Time
	Runtime0    time.Time
	ReplaySpeed float64
	client      string
	queryChan   <-chan query.Query
	dbConns     *conn.DBConns
	wg          *sync.WaitGroup
	stats       chan<- Stat
	metrics     metrics.StatsdClient
}

func NewWorker(client string, logtime0, runtime0 time.Time, replaySpeed float64, dbConns *conn.DBConns, wg *sync.WaitGroup, stats chan<- Stat, statsdClient metrics.StatsdClient) chan<- query.Query {
	queryChan := make(chan query.Query, BufferSize)

	worker := Worker{
		Logtime0:    logtime0,
		Runtime0:    runtime0,
		ReplaySpeed: replaySpeed,
		client:      client,
		queryChan:   queryChan,
		dbConns:     dbConns,
		wg:          wg,
		stats:       stats,
		metrics:     statsdClient,
	}

	wg.Add(1)
	go func() {
		time.Sleep(250 * time.Millisecond)
		worker.Run()
	}()

	return queryChan
}

func (w *Worker) AcquireDB() (conn.DBConn, error) {
	timer := metrics.NewStopwatch()
	db, err := w.dbConns.AcquireDB()
	if err != nil {
		return nil, fmt.Errorf("[%s] error from dbOpener: %v", w.client, err)
	}

	timer.Stop()
	ms := timer.Ms()
	w.metrics.Histogram("dbOpener.Open", float64(ms))
	if ms > 100 {
		logger.Infof("Took %d ms to open DB connection.", ms)
	}

	return db, nil
}

func (w *Worker) Run() {
	var db conn.DBConn

	var inTx = false
	for query := range w.queryChan {
		w.metrics.Gauge("query.chan", float64(len(w.queryChan)), "client:" + w.client)
		//query = strings.TrimSpace(query)
		//if !strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		//	//logger.Debugf("[%s] Skipping non-SELECT query: %v", query)
		//	continue
		//}
		if db == nil {
			var err error
			if db, err = w.AcquireDB(); err != nil {
				logger.Errorf("Error from AcquireDB: %v", err)
				continue
			}
		}

		if strings.HasPrefix(query.SQL, "BEGIN") {
			inTx = true
		}

		_ = w.doQuery(db, query)

		if strings.HasPrefix(query.SQL, "COMMIT") || strings.HasPrefix(query.SQL, "ROLLBACK") {
			inTx = false
		}

		if !inTx && len(w.queryChan) == 0 {
			w.dbConns.ReleaseDB(db)
			db = nil
		}
	}
	w.wg.Done()
}

func (w *Worker) doQuery(db conn.DBConn, q query.Query) error {
	replayTime := w.Runtime0.Add(q.Time.Sub(w.Logtime0) * 10 / time.Duration(10 * w.ReplaySpeed))
	now := time.Now()
	if replayTime.After(now) {
		d := replayTime.Sub(now)
		if d >= 2 * time.Second {
			logger.Infof("Query 2+ seconds in future: duration=%v; sleeping %v\n", d, d/2)
			time.Sleep(d / 2)
		}
	}

	//logger.Debugf("[%s] Querying: %s", w.client, query)
	timer := metrics.NewStopwatch()
	active := atomic.AddInt64(&concurrent, 1)

	sql := q.SQL
	rows, err := db.Query(sql)

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

	if strings.HasPrefix(sql, "SELECT  `users`.* FROM `users` WHERE `users`.`authentication_token` =") {
		stat := Stat{}
		if stat.userId, err = row.Int("id"); err != nil {
			logger.Warnf("[%s] Error extracting user 'id': %v", w.client, err)
		} else {
			w.stats <- stat
		}
	} else if strings.HasPrefix(sql, "SELECT  `oauth_tokens`.* FROM `oauth_tokens` WHERE `oauth_tokens`.`type` IN ('Oauth2Token')") {
		stat := Stat{}
		if stat.userId, err = row.Int("user_id"); err != nil {
			logger.Warnf("[%s] Error extracting 'user_id': %v", w.client, err)
		} else {
			w.stats <- stat
		}
	} else if strings.HasPrefix(sql, "SELECT  `photos`.* FROM `photos` WHERE `photos`.`id` = ") {
		stat := Stat{}
		if stat.photoId, err = row.Int("id"); err != nil {
			logger.Warnf("[%s] Error extracting photo 'id': %v", w.client, err)
		} else {
			w.stats <- stat
		}
	}
	return nil
}
