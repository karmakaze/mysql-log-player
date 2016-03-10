package worker

import (
	// "database/sql"
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/mysql-log-player/query"
)

type WorkerPool struct {
	wg          *sync.WaitGroup
	connections map[string]chan<- string
}

func NewWorkerPool() *WorkerPool {
	return &WorkerPool{
		wg:          &sync.WaitGroup{},
		connections: make(map[string]chan<- string),
	}
}

func (p *WorkerPool) Dispatch(q *query.Query) {
	workerChan, ok := p.connections[q.Client]
	if !ok {
		logger.Infof("Created new worker for client: %s", q.Client)
		workerChan = NewWorker(q.Client, p.wg)
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
