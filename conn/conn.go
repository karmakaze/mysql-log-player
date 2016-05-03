package conn


import (
	"errors"
)

var (
	NoRowsError = errors.New("no rows")
	NoColumnError = errors.New("no columns")
)

type DBOpener interface {
	Open() (DBConn, error)
}

type DBConn interface {
	Query(string, ...interface{}) (Rows, error)
	Close() error
}

type Rows interface {
	First() (Row, error)
	Close() error
}

type Row interface {
	Int(string) (int32, error)
}
