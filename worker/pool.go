package worker

import (
	"database/sql"
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/melraidin/mysql-log-player/query"
)

type WorkerPool struct {
	db          *sql.DB
	wg          *sync.WaitGroup
	connections map[string]chan<- string
	appStats    *AppStats
	metrics     metrics.StatsdClient
}

func NewWorkerPool(db *sql.DB, metrics metrics.StatsdClient) *WorkerPool {
	stats := make(chan Stat, 2000)
	appStats := NewAppStats(stats, metrics)
	appStats.Run()

	return &WorkerPool{
		db:          db,
		wg:          &sync.WaitGroup{},
		connections: make(map[string]chan<- string),
		appStats:    appStats,
		metrics:     metrics,
	}
}

func (p *WorkerPool) Dispatch(q *query.Query) {
	workerChan, ok := p.connections[q.Client]
	if !ok {
		logger.Infof("Created new worker for client: %s", q.Client)
		workerChan = NewWorker(q.Client, p.db, p.wg, p.appStats.stats, p.metrics)
		p.connections[q.Client] = workerChan
		p.metrics.Gauge("clients", float64(len(p.connections)))
	}
	workerChan <- q.SQL
}

func (p *WorkerPool) Wait() {
	for _, connection := range p.connections {
		close(connection)
	}

	p.wg.Wait()
}
