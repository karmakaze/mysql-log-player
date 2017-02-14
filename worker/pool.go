package worker

import (
	"sync"

//  logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/metrics"
	"github.com/melraidin/mysql-log-player/query"
	"github.com/melraidin/mysql-log-player/db"
)

type WorkerPool struct {
	connectInfo db.ConnectInfo
	dryRun      bool
	readOnly    bool
	updatesOnly bool
	wg          *sync.WaitGroup
	connections map[string]chan<- string
	metrics     metrics.StatsdClient
}

func NewWorkerPool(connectInfo db.ConnectInfo, dryRun bool, readOnly bool, updatesOnly bool, metrics metrics.StatsdClient) *WorkerPool {
	return &WorkerPool{
		connectInfo: connectInfo,
		dryRun:      dryRun,
		readOnly:    readOnly,
		updatesOnly: updatesOnly,
		wg:          &sync.WaitGroup{},
		connections: make(map[string]chan<- string),
		metrics:     metrics,
	}
}

func (p *WorkerPool) Dispatch(q *query.Query) {
	workerChan, ok := p.connections[q.Client]
	if !ok {
		workerChan = NewWorker(q.Client, p.connectInfo, p.dryRun, p.readOnly, p.updatesOnly, p.wg, p.metrics)
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
