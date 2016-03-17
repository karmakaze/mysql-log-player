package worker

import (
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
	"database/sql"
	"strings"
)

var (
	// Size of command buffer for new workers.
	BufferSize = 20
)

type Worker struct {
	client    string
	queryChan <-chan string
	db        *sql.DB
	wg        *sync.WaitGroup
}

func NewWorker(client string, db *sql.DB, wg *sync.WaitGroup) chan<- string {
	queryChan := make(chan string, BufferSize)

	worker := Worker{
		client:    client,
		queryChan: queryChan,
		db:        db,
		wg:        wg,
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
		logger.Debugf("[%s] Querying: %s", w.client, query)

		rows, err := w.db.Query(query)
		if err != nil {
			logger.Warnf("error '%v' running query: %s", err, query)
			continue
		}
		err = rows.Close()
		if err != nil {
			logger.Warnf("error closing rows: %v", err)
		}
	}
	w.wg.Done()
}
