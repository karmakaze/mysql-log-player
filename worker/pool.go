package worker

import (
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/melraidin/mysql-log-player/conn"
	"github.com/melraidin/mysql-log-player/query"
)

type Concurrency struct {
	limit int
	count int
	cond  *sync.Cond
}

func (c *Concurrency) Inc() int {
	defer c.cond.L.Unlock()
	c.cond.L.Lock()

	for c.count >= c.limit {
		c.cond.Wait()
	}
	c.count += 1
	return c.count
}

func (c *Concurrency) Dec() int {
	defer c.cond.L.Unlock()
	c.cond.L.Lock()

	c.count -= 1
	c.cond.Broadcast()
	return c.count
}

type WorkerPool struct {
	dbOpener    conn.DBOpener
	wg          *sync.WaitGroup
	concurrency *Concurrency
	connections map[string]chan<- string
	appStats    *AppStats
	metrics     metrics.StatsdClient
}

func NewWorkerPool(dbOpener conn.DBOpener, limit int, metrics metrics.StatsdClient) *WorkerPool {
	stats := make(chan Stat, 2000)
	appStats := NewAppStats(stats, metrics)
	go appStats.Run()

	return &WorkerPool{
		dbOpener:    dbOpener,
		wg:          &sync.WaitGroup{},
		concurrency: &Concurrency{limit, 0, sync.NewCond(&sync.Mutex{})},
		connections: make(map[string]chan<- string),
		appStats:    appStats,
		metrics:     metrics,
	}
}

func (p *WorkerPool) Dispatch(q *query.Query) {
	workerChan, ok := p.connections[q.Client]
	if !ok {
		logger.Infof("Created new worker for client: %s", q.Client)
		workerChan = NewWorker(q.Client, p.dbOpener, p.concurrency, p.wg, p.appStats.stats, p.metrics)
		p.connections[q.Client] = workerChan
		p.metrics.Gauge("clients", float64(len(p.connections)))
	}
	workerChan <- q.SQL
	p.appStats.ReportSpeed(q.Time)
}

func (p *WorkerPool) Wait() {
	for _, connection := range p.connections {
		close(connection)
	}

	p.wg.Wait()
}
