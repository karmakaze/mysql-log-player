package worker

import ()

type QueryWorker struct {
	id        int
	queryChan <-chan (string)
	wg        *sync.WaitGroup
}

func NewQueryWorker(queryChan <-chan (string), wg *sync.WaitGroup) *QueryWorker {
	return &QueryWorker{
		id:        nextQueryWorkerID()(),
		queryChan: queryChan,
		wg:        wg,
	}
}

func nextQueryWorkerID() func() int {
	lastID := -1
	return func() int {
		fmt.Printf("lastID: %d\n", lastID)
		lastID++
		return lastID
	}
}

func (w *QueryWorker) Run() {
	fmt.Printf("[%d] Started\n", w.id)
	for query := range w.queryChan {
		fmt.Printf("[%d] Read query: %s\n", w.id, query)
		logger.Debugf("[%d] Querying: %s\n", w.id, query)
	}
	w.wg.Done()
}
