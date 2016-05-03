package conn

import (
	"sync"
)

type DBConns struct {
	maxConnections int
	made           int
	conns          chan DBConn
	dbOpener       DBOpener
	lock           sync.Mutex
}

func NewDBConns(maxConnections int, dbOpener DBOpener) *DBConns {
	return &DBConns{maxConnections, 0, make(chan DBConn, maxConnections), dbOpener, sync.Mutex{}}
}

func (dbs *DBConns) AcquireDB() (DBConn, error) {
	select {
	case db := <-dbs.conns:
		return db, nil
	default:
	}

	dbs.lock.Lock()
	if dbs.made < dbs.maxConnections {
		dbs.made++
		dbs.lock.Unlock()
		return dbs.dbOpener.Open()
	}
	dbs.lock.Unlock()
	return <-dbs.conns, nil
}

func (dbs *DBConns) ReleaseDB(db DBConn) {
	dbs.conns<- db
}
