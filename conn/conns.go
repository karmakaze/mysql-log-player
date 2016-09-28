package conn

import (
	"sync"
	"fmt"
)

type DBConns struct {
    dbOpener DBOpener
	conns    map[DBConn]struct{}
	lock     sync.Mutex
}

func NewDBConns(dbOpener DBOpener) *DBConns {
	return &DBConns{dbOpener, nil, sync.Mutex{}}
}

func (dbs *DBConns) DBOpen() (DBConn, error) {
	db, err := dbs.dbOpener.Open()
	if err != nil {
		return nil, err
	}
	dbs.conns[db] = struct{}{}
	return db, nil
}

func (dbs *DBConns) AcquireDB() (DBConn, error) {
	return nil, fmt.Errorf("not implemented")
}

func (dbs *DBConns) ReleaseDB(db DBConn) {
}
