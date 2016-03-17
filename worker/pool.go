package worker

import (
	// "database/sql"
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/melraidin/mysql-log-player/query"
	"database/sql"
)

type WorkerPool struct {
	db          *sql.DB
	wg          *sync.WaitGroup
	connections map[string]chan<- string
}

func NewWorkerPool(db *sql.DB) *WorkerPool {
	return &WorkerPool{
		db:          db,
		wg:          &sync.WaitGroup{},
		connections: make(map[string]chan<- string),
	}
}

func (p *WorkerPool) Dispatch(q *query.Query) {
	workerChan, ok := p.connections[q.Client]
	if !ok {
		logger.Infof("Created new worker for client: %s", q.Client)
		workerChan = NewWorker(q.Client, p.db, p.wg)
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
