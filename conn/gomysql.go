package conn

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type GoMySQLOpener struct {
	driver         string
	connectionInfo string
}
type GoMySQLConn struct { db *sql.DB }
type GoMySQLRows struct { rows *sql.Rows }
type GoMySQLRow  struct { row *sql.Rows }

func NewGoMySQLOpener(driver string, connectionInfo string) DBOpener {
	return GoMySQLOpener{
		driver:         driver,
		connectionInfo: connectionInfo,
	}
}

func (g GoMySQLOpener) Open() (DBConn, error) {
	db, err := sql.Open(g.driver, g.connectionInfo)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return GoMySQLConn{db}, nil
}

func (c GoMySQLConn) Query(query string, params ...interface{}) (Rows, error) {
	rows, err := c.db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	return GoMySQLRows{rows}, nil
}

func (rs GoMySQLRows) Close() error {
	return rs.rows.Close()
}

func (rs GoMySQLRows) First() (Row, error) {
	if !rs.rows.Next() {
		return nil, NoRowsError
	}
	return GoMySQLRow{rs.rows}, nil
}

func (r GoMySQLRow) Int(name string) (int32, error) {
	colNames, err := r.row.Columns()
	if err != nil {
		return 0, err
	}
	if len(colNames) == 0 {
		return 0, NoColumnError
	}

	var intValue int32
	values := make([]interface{}, len(colNames))
	for i, colName := range colNames {
		if colName == name {
			values[i] = &intValue
		} else {
			var iface interface{}
			values[i] = &iface
		}
	}
	if err = r.row.Scan(values...); err != nil {
		return 0, err
	}
	return intValue, nil
}
