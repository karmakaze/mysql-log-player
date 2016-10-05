package worker

import (
	"database/sql"
	"sync"

//      logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/melraidin/mysql-log-player/query"
)

type WorkerPool struct {
	db          *sql.DB
	dryRun      bool
	readOnly    bool
	wg          *sync.WaitGroup
	connections map[string]chan<- string
	appStats    *AppStats
	metrics     metrics.StatsdClient
}

func NewWorkerPool(db *sql.DB, dryRun bool, readOnly bool, metrics metrics.StatsdClient) *WorkerPool {
	stats := make(chan Stat, 2000)
	appStats := NewAppStats(stats, metrics)
	go appStats.Run()

	return &WorkerPool{
		db:          db,
		dryRun:      dryRun,
		readOnly:    readOnly,
		wg:          &sync.WaitGroup{},
		connections: make(map[string]chan<- string),
		appStats:    appStats,
		metrics:     metrics,
	}
}

func (p *WorkerPool) Dispatch(q *query.Query) {
	workerChan, ok := p.connections[q.Client]
	if !ok {
		workerChan = NewWorker(q.Client, p.db, p.dryRun, p.readOnly, p.wg, p.appStats.stats, p.metrics)
		p.connections[q.Client] = workerChan
	}
	workerChan <- q.SQL
}

func (p *WorkerPool) Wait() {
	for _, connection := range p.connections {
		close(connection)
	}

	p.wg.Wait()
}
