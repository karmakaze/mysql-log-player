package worker

import (
	"sync"

	logger "github.com/500px/go-utils/chatty_logger"
)

var (
	// Size of command buffer for new workers.
	BufferSize = 20
)

type Worker struct {
	client    string
	queryChan <-chan string
	wg        *sync.WaitGroup
}

func NewWorker(client string, wg *sync.WaitGroup) chan<- string {
	queryChan := make(chan string, BufferSize)

	worker := Worker{
		client:    client,
		queryChan: queryChan,
		wg:        wg,
	}

	wg.Add(1)
	go worker.Run()

	return queryChan
}

func (w *Worker) Run() {
	for query := range w.queryChan {
		logger.Debugf("[%s] Querying: %s\n", w.client, query)
	}
	w.wg.Done()
}
